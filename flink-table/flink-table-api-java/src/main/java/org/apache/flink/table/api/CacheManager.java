package org.apache.flink.table.api;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.TableSinkSourceFactory;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.OperationTreeBuilder;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.Preconditions;

import java.util.*;

public class CacheManager {
	private TableSinkSourceFactory tableSinkSourceFactory;
	private TableEnvironment tableEnvironment;
	private Map<QueryOperation, String> cachingTables = new HashMap<>();
	private Map<QueryOperation, String> cachedTables = new HashMap<>();

	public CacheManager(TableEnvironment tableEnvironment) {
		this.tableEnvironment = tableEnvironment;
	}

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
			ConnectorCatalogTable.sourceAndSink(tableSinkSourceFactory.createTableSource(sinkSourceConf),
				tableSinkSourceFactory.createTableSink(sinkSourceConf),
				true);
		Catalog catalog = tableEnvironment.getCatalog(tableEnvironment.getCurrentCatalog()).orElse(null);

		Preconditions.checkNotNull(catalog);

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

	public void finishAllCaching() {
		cachedTables.putAll(cachingTables);
		cachingTables.clear();
	}

	public void registerCacheStorage(TableSinkSourceFactory tableSinkSourceFactory) {
		this.tableSinkSourceFactory = tableSinkSourceFactory;
	}

	public QueryOperation buildOperationTree(OperationTreeBuilder builder, QueryOperation queryOperation) {
		if (cachedTables.containsKey(queryOperation)) {
			// table is in cached
			String tableId = cachedTables.get(queryOperation);
			return tableEnvironment.scan(tableId).getQueryOperation();
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
