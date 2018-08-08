package com.github.mmolimar.kafka.connect.fs.util;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class ReflectionUtils {

    public static FileReader makeReader(Class<? extends FileReader> clazz, FileSystem fs,
                                        Path path, Map<String, Object> config) throws IOException {
        final FileReader reader = make(clazz, fs, path);

        Map<String, Object> readerConf = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILE_READER_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
        reader.configure(readerConf);
        return reader;
    }

    public static Policy makePolicy(Class<? extends Policy> clazz, FsSourceTaskConfig conf) throws IOException {
        return make(clazz, conf);
    }

    private static <T> T make(Class<T> clazz, Object... args) throws IOException {
        Class[] constClasses = Arrays.stream(args).map(arg -> arg.getClass()).toArray(Class[]::new);

        try {
            Constructor constructor = ConstructorUtils.getMatchingAccessibleConstructor(clazz, constClasses);
            return (T) constructor.newInstance(args);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException(String.format("Cannot instantiate reader %s with given arguments %s", clazz, Arrays.asList(args)), e);
        } catch (InvocationTargetException e) {
            if(e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw (RuntimeException) e.getCause();
        }
    }
}
