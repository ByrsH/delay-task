package com.byrsh.delaytask.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Author: yangrusheng
 * @Description:
 * @Date: Created in 18:37 2019/8/17
 * @Modified By:
 */
public class ScriptUtil {

    private ScriptUtil() {}

    public static String readScript(String resourceName) throws IOException {
        try (final InputStream inputStream = ScriptUtil.class.getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Could not find script resource: " + resourceName);
            }
            int size = inputStream.available();
            byte[] buffer = new byte[size];
            inputStream.read(buffer);
            return new String(buffer, "utf-8");
        }
    }
}
