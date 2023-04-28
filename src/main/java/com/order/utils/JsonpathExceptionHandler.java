package com.order.utils;

import com.jayway.jsonpath.JsonPath;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class is just a helper to allow Jsonpath for easier use.
 * You can extract exact values, an object or an array depending on the Jsonpath expression used.
 * If a path is not found, an exception is throw. Instead of throwing an exception, an empty value or array is returned
 * where applicable.
 */

public class JsonpathExceptionHandler {
	public JsonpathExceptionHandler() {
	}

	public static List<Map<String, Object>> getList(Object document, String path) {
		try {
			return JsonPath.read(document, path);
		} catch (Exception e) {
			return Collections.emptyList();
		}
	}

	public static String getValue(Object document, String path) {
		try {
			return JsonPath.read(document, path).toString();
		} catch (Exception e) {
			return "";
		}
	}

	public static List<String> getStringList(Object document, String path) {
		try {
			return JsonPath.read(document, path);
		} catch (Exception e) {
			return Collections.emptyList();
		}
	}

	public static Map<String, Object> getObject(Object document, String path) {
		try {
			return JsonPath.read(document, path);
		} catch (Exception e) {
			return Collections.emptyMap();
		}
	}
}
