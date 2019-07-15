package org.apache.flink.table.api;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.table.utils.ConfigUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CacheManager {

	private static final Logger log = LoggerFactory.getLogger(CacheManager.class);

	private static final String CONF_FILE_NAME = "intermediate-result-storage-conf.yaml";
	private final Catalog catalog;
	private IntermediateResultStorage intermediateResultStorage;
	private TableEnvironment tableEnvironment;
	private Map<QueryOperation, String> cachingTables = new HashMap<>();
	private Map<QueryOperation, String> cachedTables = new HashMap<>();

	public static final String CATALOG_NAME = "CACHE_TABLE_CATALOG";
	public static final String DEFAULT_DATABASE = "DEFAULT";

	public CacheManager(TableEnvironment tableEnvironment) {
		this.tableEnvironment = tableEnvironment;
		tableEnvironment.registerCatalog(CATALOG_NAME, new GenericInMemoryCatalog(CATALOG_NAME, DEFAULT_DATABASE));
		this.catalog = tableEnvironment.getCatalog(CATALOG_NAME).orElse(null);
		Preconditions.checkNotNull(this.catalog);

		loadIntermediateResultStorageIfExist();
	}

	private void loadIntermediateResultStorageIfExist() {

		log.info("loading {} from the classpath", CONF_FILE_NAME);
		URL url = getClass().getClassLoader().getResource(CONF_FILE_NAME);
		if (url == null) {
			log.info("{} is not found", CONF_FILE_NAME);
			return;
		}
		ObjectMapper objectMapper = new ConfigUtil.LowerCaseYamlMapper();
		try {
			DescriptorProperties descriptorProperties =
				ConfigUtil.normalizeYaml(objectMapper.readValue(url, Map.class));
			TableFactoryService tableFactoryService = new TableFactoryService();
			IntermediateResultStorage intermediateResultStorage =
				tableFactoryService.find(IntermediateResultStorage.class, descriptorProperties.asMap());
			registerCacheStorage(intermediateResultStorage);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Register the table to be cached, the data is not yet write to the sink.
	 * @param table the table to be cached after the job finished.
	 * @return the Id of the cached table
	 */
	public String registerTableToCache(Table table) {
		if (isCaching(table)) {
			return cachingTables.get(table.getQueryOperation());
		}

		if (isCached(table)) {
			return cachedTables.get(table.getQueryOperation());
		}

		String id = UUID.randomUUID().toString();
		cachingTables.put(table.getQueryOperation(), id);
		registerTableToCatalog(id, table);
		return id;
	}

	private void registerTableToCatalog(String id, Table table) {
		Map<String, String> sinkSourceConf =
			intermediateResultStorage.getTableCreationHook().createTable(id, getSinkSourceConf(table));

		ConnectorCatalogTable connectorCatalogTable =
			ConnectorCatalogTable.sourceAndSink(
				intermediateResultStorage.getTableSourceFactory().createTableSource(sinkSourceConf),
				intermediateResultStorage.getTableSinkFactory().createTableSink(sinkSourceConf),
				true);

		try {
			catalog.createTable(new ObjectPath(catalog.getDefaultDatabase(), id), connectorCatalogTable,
				true);
		} catch (TableAlreadyExistException e) {
			e.printStackTrace();
		} catch (DatabaseNotExistException e) {
			e.printStackTrace();
		}
	}

	private Map<String, String> getSinkSourceConf(Table table) {
		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putTableSchema(Schema.SCHEMA, table.getSchema());
		return descriptorProperties.asMap();
	}

	private boolean isCached(Table table) {
		return cachedTables.containsKey(table.getQueryOperation());
	}

	private boolean isCaching(Table table) {
		return cachingTables.containsKey(table.getQueryOperation());
	}

	public void tableCached(Table table) {
		if (!isCaching(table)) {
			throw new RuntimeException("Table is not in caching");
		}

		String id = cachingTables.remove(table.getQueryOperation());
		cachedTables.put(table.getQueryOperation(), id);
	}

	/**
	 * Move all tables from cachingTable to cachedTable so that the CacheManager can replace the cached table with
	 * a table source upon a submission of another job.
	 */
	public void markAllTableCached() {
		cachedTables.putAll(cachingTables);
		cachingTables.clear();
	}

	/**
	 * Register the TableSourceFactory and TableSinkFactory, which will be used to create TableSource and TableSink to
	 * read from and write to the external cache storage.
	 * @param intermediateResultStorage
	 */
	public void registerCacheStorage(IntermediateResultStorage intermediateResultStorage) {
		this.intermediateResultStorage = intermediateResultStorage;
	}

	/**
	 * Go through the operation tree and replace the cached table and its subtree with a table source
	 * @param builder used to build the operation tree
	 * @param queryOperation the root of the operation tree
	 * @return the new operation tree, in which the cached table is replaced
	 */
	public QueryOperation buildOperationTree(OperationTreeBuilder builder, QueryOperation queryOperation) {
		if (cachedTables.containsKey(queryOperation)) {
			// table is in cached
			String tableId = cachedTables.get(queryOperation);
			return tableEnvironment.scan(CATALOG_NAME, DEFAULT_DATABASE, tableId).getQueryOperation();
		}

		List<QueryOperation> children = new ArrayList<>();
		boolean childReplaced = false;
		for (QueryOperation child : queryOperation.getChildren()) {
			QueryOperation newQueryOperation = buildOperationTree(builder, child);
			children.add(newQueryOperation);
			if (newQueryOperation != child) {
				childReplaced = true;
			}
		}

		if (childReplaced) {
			return replaceQueryOperationChildren(builder, queryOperation, children);
		}

		return queryOperation;
	}

	private QueryOperation replaceQueryOperationChildren(OperationTreeBuilder builder, QueryOperation queryOperation,
														 List<QueryOperation> children) {


		if (queryOperation instanceof ProjectQueryOperation) {
			ProjectQueryOperation projectQueryOperation = (ProjectQueryOperation) queryOperation;
			List<Expression> expressionList = new ArrayList<>();
			for (ResolvedExpression resolvedExpression : projectQueryOperation.getProjectList()) {
				expressionList.add(resolvedExpression);
			}
			return builder.project(expressionList, children.get(0));
		} else if (queryOperation instanceof FilterQueryOperation) {
			FilterQueryOperation filterQueryOperation = (FilterQueryOperation) queryOperation;
			return builder.filter(filterQueryOperation.getCondition(), children.get(0));
		}
		// TODO: more type of QueryOperation to handle

		return queryOperation;
	}
}
