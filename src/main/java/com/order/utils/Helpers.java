package com.order.utils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

public class Helpers
{
	private Helpers()
	{
	}

	/**
	 * Sometimes a payload might be needed to be compressed for better data management.
	 * This function takes a stringified payload as input and produces gzip string.
	 */
	public static byte[] compress(String str) throws Exception
	{
		try
		{
			if (str == null || str.length() == 0)
			{
				return null;
			}
			ByteArrayOutputStream obj = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(obj);
			gzip.write(str.getBytes(StandardCharsets.UTF_8));
			gzip.close();

			return obj.toByteArray();
		}
		catch (Exception e)
		{
			throw new Exception("Couldn't compress input feed");
		}

	}

}