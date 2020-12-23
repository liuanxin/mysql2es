package com.github.util;

import com.github.model.Const;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class F {

    private static String getFileName(String table, String index) {
        StringBuilder sbd = new StringBuilder();
        sbd.append(index);
        String tableToIndex = U.tableToIndex(table);
        if (!tableToIndex.equals(index)) {
            sbd.append("-").append(tableToIndex);
        }
        return U.addSuffix(Const.TMP) + sbd.toString();
    }

    public static String read(String table, String index) {
        String fileName = getFileName(table, index);
        try {
            String content = Files.asCharSource(new File(fileName), U.UTF8).read();
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("read content({}) with file ({})", content, fileName);
            }
            return content;
        } catch (FileNotFoundException e) {
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("no file ({})", fileName);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("read from file(%s) exception", fileName), e);
            }
        }
        return U.EMPTY;
    }

    public static void write(String table, String index, String content) {
        String fileName = getFileName(table, index);
        try {
            Files.asCharSink(new File(fileName), U.UTF8).write(content);
            if (Logs.ROOT_LOG.isDebugEnabled()) {
                Logs.ROOT_LOG.debug("write content({}) to file ({})", content, fileName);
            }
        } catch (IOException e) {
            if (Logs.ROOT_LOG.isErrorEnabled()) {
                Logs.ROOT_LOG.error(String.format("write to file(%s) exception", fileName), e);
            }
        }
    }

    public static void delete(String table, String index) {
        String fileName = getFileName(table, index);
        boolean flag = new File(fileName).delete();
        if (Logs.ROOT_LOG.isDebugEnabled()) {
            Logs.ROOT_LOG.debug("delete ({}) {}", fileName, flag);
        }
    }
}
