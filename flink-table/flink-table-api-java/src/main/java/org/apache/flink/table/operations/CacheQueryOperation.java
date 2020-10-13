package org.apache.flink.table.operations;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.AbstractID;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CacheQueryOperation implements QueryOperation {

	private final QueryOperation child;
	private final AbstractID intermediateDataSetId;

	public CacheQueryOperation(QueryOperation child,
							   AbstractID intermediateDataSetId) {
		this.child = child;
		this.intermediateDataSetId = intermediateDataSetId;
	}

	public AbstractID getIntermediateDataSetId() {
		return intermediateDataSetId;
	}

	public QueryOperation getChild() {
		return child;
	}

	@Override
	public TableSchema getTableSchema() {
		return child.getTableSchema();
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.singletonList(child);
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("intermediateDataSetId", intermediateDataSetId);

		return OperationUtils.formatWithChildren("Cache", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}
}
