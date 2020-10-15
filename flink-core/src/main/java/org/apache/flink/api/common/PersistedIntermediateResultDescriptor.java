package org.apache.flink.api.common;

import org.apache.flink.util.AbstractID;

import java.io.Serializable;

public interface PersistedIntermediateResultDescriptor extends Serializable {
	AbstractID getIntermediateDataSetId();
}
