package com.talend.lambda.handlers;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
public class TacResponse {
	@JsonIgnoreType
	public static class ExecutionTimeType{
		public long millis;
		public String seconds;
	}
	
	public Integer returnCode;
	public Integer taskId;
	public ExecutionTimeType executionTime;
	
}