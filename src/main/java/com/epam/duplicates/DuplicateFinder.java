package com.epam.duplicates;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class DuplicateFinder {

    private Path path;

    public DuplicateFinder(Path path) {
        this.path = path;
    }

    public static void main(String[] args) {
        Path path;
        System.out.println("Please, supply a directory path");
        Scanner scanner = new Scanner(System.in);
        String paths = scanner.next();
        path = Paths.get(paths);
        DuplicateFinder duplicateFinder = new DuplicateFinder(path);
        Map<Long, List<Path>> filesList = new HashMap<>();
        duplicateFinder.directoryWalker(filesList, path);

        for (Map.Entry<Long, List<Path>> item : filesList.entrySet()) {
            List<Path> valueList = item.getValue();
            if (valueList.size() > 1) {
                duplicateFinder.findDuplicatesAndMakeHardLink(valueList, item.getKey());
            }
        }
    }

    private void directoryWalker(Map<Long, List<Path>> filesList, Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!attrs.isDirectory()) {
                        List<Path> pathList = filesList.computeIfAbsent(attrs.size(), k -> new ArrayList<>());
                        pathList.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void findDuplicatesAndMakeHardLink(List<Path> paths, Long size) {
        for (int i = 0; i < paths.size(); i++) {
            for (int j = i + 1; j < paths.size(); j++) {
                Path original = paths.get(i);
                Path duplicate = paths.get(j);
                if (compareFiles(original, duplicate)) {
                    System.out.println("ORIGINAL FILE: " + original);
                    System.out.println("DUPLICATE FILE: " + duplicate);
                    System.out.println("FILE SIZE: " + size + " bytes" + "\n");

                    try {
                        Files.delete(duplicate);
                        Files.createLink(duplicate, original);
                    } catch (IOException | UnsupportedOperationException x) {
                        System.err.println(x);
                    }
                }
            }
        }
    }

    private boolean compareFiles(Path path1, Path path2) {
        boolean isDuplicate = true;
        try (final FileInputStream fileContent1 = new FileInputStream(path1.toString())) {
            try (final FileInputStream fileContent2 = new FileInputStream(path2.toString())) {
                int bytes1, bytes2;
                while ((bytes1 = fileContent1.read()) != -1) {
                    bytes2 = fileContent2.read();
                    if (bytes1 != bytes2) {
                        isDuplicate = false;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
        return isDuplicate;
    }
}
