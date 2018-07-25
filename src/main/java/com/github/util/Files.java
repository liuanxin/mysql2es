package com.github.util;

import com.github.model.Const;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Files {

    private static String getFileName(String index) {
        return U.addSuffix(Const.TMP) + index;
    }

    public static String read(String index) {
        String fileName = getFileName(index);
        try {
            return com.google.common.io.Files.asCharSource(new File(fileName), U.UTF8).read();
        } catch (FileNotFoundException e) {
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("no file ({})", fileName);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info(String.format("read from file(%s) exception", fileName), e);
            }
        }
        return U.EMPTY;
    }

    public static boolean write(String index, String content) {
        String fileName = getFileName(index);
        try {
            com.google.common.io.Files.asCharSink(new File(fileName), U.UTF8).write(content);
            return true;
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isInfoEnabled()) {
                Logs.ROOT_LOG.info(String.format("write to file(%s) exception", fileName), e);
            }
            return false;
        }
    }

    public static boolean delete(String index) {
        String fileName = getFileName(index);
        boolean flag = new File(fileName).delete();
        if (Logs.ROOT_LOG.isInfoEnabled()) {
            Logs.ROOT_LOG.info("delete ({}) {}", fileName, flag);
        }
        return flag;
    }
}
