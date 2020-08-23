package com.talend.lambda.handlers;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
public class S3LambdaFunctionHandler implements RequestHandler<S3Event, Object> {
	@Override
	public Object handleRequest(S3Event pS3Event, Context context) {
		final String v_tac_endpoint = System.getenv("tac_endpoint");
		final String v_tac_username = System.getenv("tac_username");
		final String v_tac_password = System.getenv("tac_password");
		final String v_tac_task_name = System.getenv("tac_task_name");
		
		Integer vTaskId = null;
		String v_bucket = null;
		String v_object = null;
		CloseableHttpClient v_http_client = null;
		CloseableHttpResponse v_response = null;
		S3EventNotificationRecord v_record = null;
		
		
		// To keep things simple for this demo,
		// we only handle the first S3 event notification in the list
		List<S3EventNotificationRecord> v_liste_records = pS3Event.getRecords();
		if (v_liste_records != null && v_liste_records.size() > 0) {
			v_record = v_liste_records.get(0);
		}
		if (v_record != null) {
			v_bucket = v_record.getS3().getBucket().getArn();
			context.getLogger().log("bucket = " + v_bucket);
			v_object = v_record.getS3().getObject().getKey();
			context.getLogger().log("file = " + v_object);
		}
		// if an object has been uploaded in S3, call TAC Metaservlet to trigger
		// Talend Job
		if (v_object != null) {
			v_http_client = HttpClients.createDefault();
			// get the tac task id by name
			String v_taskid_request = "{\"actionName\":\"getTaskIdByName\",\"authPass\":\""
					+ v_tac_password + "\",\"authUser\":\"" + v_tac_username + "\",\"taskName\":\"" + v_tac_task_name
					+ "\"}";
			context.getLogger().log("Request being sent to Talend Administration Center : " + v_taskid_request);
			String v_taskid_request_base64 = Base64.getEncoder().encodeToString(v_taskid_request.getBytes());
			HttpGet v_http_get_taskid_request = new HttpGet(
					v_tac_endpoint + "/metaServlet?" + v_taskid_request_base64);
			try {
				// send the HTTP request to remote TAC
				v_response = v_http_client.execute(v_http_get_taskid_request);
				// log response received from TAC
				String v_json = EntityUtils.toString(v_response.getEntity());
				context.getLogger().log("Response from Talend Administration Center : " + v_json);
				// Extract response
				ObjectMapper vObjectMapper = new ObjectMapper();
				TacResponse response = vObjectMapper.readValue(v_json, TacResponse.class);
				if(response != null){
					vTaskId = response.taskId;
				}
			} catch (ClientProtocolException e_ClientProtocolException) {
				context.getLogger().log(e_ClientProtocolException.toString());
			} catch (IOException e_IOException) {
				context.getLogger().log(e_IOException.toString());
			}
		}
		
		//if the task id has been retrieved
		if (vTaskId != null) {
			String v_tac_run_task_request = "{\"actionName\":\"runTask\",\"authPass\":\"" + v_tac_password
					+ "\",\"authUser\":\"" + v_tac_username + "\",\"taskId\":" + vTaskId
					+ ",\"mode\":\"asynchronous\"" + ",\"context\":{\"s3_file\":\"" + v_object + "\"}" + "}";
			context.getLogger().log("Request being sent to Talend Administration Center : " + v_tac_run_task_request);
			// encode the metaservlet request in base64
			String v_request_base64 = Base64.getEncoder().encodeToString(v_tac_run_task_request.getBytes());
			// construct an HTTP request
			HttpGet v_http_get_request = new HttpGet(v_tac_endpoint + "/metaServlet?" + v_request_base64);
			try {
				// send the HTTP request to remote TAC
				v_response = v_http_client.execute(v_http_get_request);
				// log response received from TAC
				context.getLogger()
						.log("Response from Talend Administration Center : " + EntityUtils.toString(v_response.getEntity()));
			} catch (ClientProtocolException e_ClientProtocolException) {
				context.getLogger().log(e_ClientProtocolException.toString());
			} catch (IOException e_IOException) {
				context.getLogger().log(e_IOException.toString());
			}
		}
		return null;
	}
}