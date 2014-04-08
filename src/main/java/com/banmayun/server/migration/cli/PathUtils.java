package com.banmayun.server.migration.cli;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

public class PathUtils {

    public static final String UNIX_SEPARATOR = "/";
    public static final char UNIX_SEPARATOR_CHAR = '/';
    public static final String WINDOWS_SEPARATOR = "\\";
    public static final char WINDOWS_SEPARATOR_CHAR = '\\';
    public static final String SEPARATOR = UNIX_SEPARATOR;
    public static final char SEPARATOR_CHAR = UNIX_SEPARATOR_CHAR;
    public static final String SEPARATORS = SEPARATOR + WINDOWS_SEPARATOR;
    public static final String ILLEGAL_CHARS = "\"*/:<>?\\|";
    public static final String ROOT_PATH = "";

    public static String concat(String basePath, String relPath) {
        if (StringUtils.startsWithAny(relPath, SEPARATORS)) {
            return normalize(relPath);
        } else {
            return normalize(basePath + SEPARATOR + relPath);
        }
    }

    public static String normalize(String path) throws IllegalArgumentException {
        if (path == null) {
            return null;
        }

        String[] segs = StringUtils.split(path, SEPARATORS);
        List<String> names = new ArrayList<String>(segs.length < 32 ? segs.length : 32);
        for (String seg : segs) {
            if (seg.equals("")) {
                continue;
            }
            if (seg.equals(".")) {
                continue;
            }
            if (seg.equals("..")) {
                if (names.size() == 0) {
                    continue;
                }
                names.remove(names.size() - 1);
                continue;
            }

            names.add(validateName(seg));
        }

        StringBuilder sb = new StringBuilder();
        for (String name : names) {
            sb.append(SEPARATOR);
            sb.append(name);
        }
        return sb.toString();
    }

    public static String getName(String path) {
        return FilenameUtils.getName(path);
    }

    public static String getParentPath(String path) {
        String parentPath = FilenameUtils.getPath(path);
        return normalize(parentPath);
    }

    public static String getExtension(String path) {
        return FilenameUtils.getExtension(path);
    }

    private static String validateName(String name) {
        name = StringUtils.strip(name);
        byte[] nameBytes = name.getBytes();
        for (byte b : nameBytes) {
            if ((b >= 0 && b <= 31) || (b == 127)) {
                throw new IllegalArgumentException("name contains unprintable character(s)");
            }
        }
        if (name.startsWith(".")) {
            throw new IllegalArgumentException("name starts with a dot");
        }
        name = StringUtils.strip(name, ".");
        if (StringUtils.contains(name, "\"*/:<>?\\|")) {
            throw new IllegalArgumentException("name contains illegal character(s)");
        }
        return name;
    }

    public static boolean isRoot(String path) {
        return normalize(path).equals("");
    }

    public static boolean equals(String path1, String path2) {
        return StringUtils.equalsIgnoreCase(normalize(path1), normalize(path2));
    }

    public static boolean directoryContains(String canonicalParent, String canonicalChild) {
        canonicalParent = normalize(canonicalParent);
        canonicalChild = normalize(canonicalChild);
        return (!StringUtils.equalsIgnoreCase(canonicalParent, canonicalChild))
                && StringUtils.startsWithIgnoreCase(normalize(canonicalChild), normalize(canonicalParent) + SEPARATOR);
    }

    public static String getTopLevelPath(String path) {
        String segs[] = StringUtils.split(normalize(path), SEPARATOR_CHAR);
        if (segs.length < 1) {
            return ROOT_PATH;
        } else {
            return SEPARATOR + segs[0];
        }
    }
}
