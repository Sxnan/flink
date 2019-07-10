package org.apache.flink.table.api;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.OperationTreeBuilder;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class CacheManager {

	private final Catalog catalog;
	private TableSourceFactory tableSourceFactory;
	private TableSinkFactory tableSinkFactory;
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
		Map<String, String> sinkSourceConf = getSinkSourceConf(id, table);
		ConnectorCatalogTable connectorCatalogTable =
			ConnectorCatalogTable.sourceAndSink(tableSourceFactory.createTableSource(sinkSourceConf),
				tableSinkFactory.createTableSink(sinkSourceConf),
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

	private Map<String, String> getSinkSourceConf(String tableName, Table table) {
		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putTableSchema("__schema__", table.getSchema());
		descriptorProperties.putString("__table__name__", tableName);
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
	 * @param tableSourceFactory table source factory to create table source
	 * @param tableSinkFactory table sink factory to create table sink
	 */
	public void registerCacheStorage(TableSourceFactory tableSourceFactory,
										  TableSinkFactory tableSinkFactory) {
		this.tableSourceFactory = tableSourceFactory;
		this.tableSinkFactory = tableSinkFactory;
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
