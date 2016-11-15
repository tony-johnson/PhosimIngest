package org.lsst.phosimingest;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.util.Collections;
import java.util.regex.Pattern;
import nom.tam.fits.FitsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

/**
 * Main routine of the PhosimIngest program.
 * @author tonyj
 */
public class PhosimIngest {

    private static final Pattern fitsPattern = Pattern.compile(".*\\.fits(\\.gz)?");
    private final Options options = new Options();

    static class Options {
        @Option(name = "-c", usage = "replace existing output files")
         boolean clobber;

        @Option(name = "-u", usage = "only link new files, update existing database")
         boolean update;

        @Option(name = "-v", usage = "verbose output")
         boolean verbose;

        @Option(name = "-o", usage = "directory where output will be put")
        @SuppressWarnings("FieldMayBeFinal")
        private File out = new File(".");

        @Option(name = "-i", usage = "directory to search for fits files")
        @SuppressWarnings("FieldMayBeFinal")
        private File in = new File(".");
    }

    public static void main(String[] args) throws IOException, SQLException {
        new PhosimIngest().doMain(args);
    }

    private void doMain(String[] args) throws IOException, SQLException {
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
            FitsFactory.setUseHierarch(true);
            Path input = options.in.toPath();
            Path output = options.out.toPath();
            Files.createDirectories(output);
            createMapper(output);
            try (Registry registry = new Registry(output, options)) {
                scanForFitsFiles(input, (file) -> handleFitsFile(file, output, registry));
            }
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("java PhosimIngest [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java PhosimIngest" + parser.printExample(OptionHandlerFilter.ALL));
        }

    }

    private void handleFitsFile(Path file, Path output, Registry registry) throws IOException {

        Visit visit = new Visit(file);
        visit.createLink(output, options);
        registry.addVisit(visit);
    }

    private void createMapper(Path output) throws IOException {
        Files.write(output.resolve("_mapper"), Collections.singletonList("lsst.obs.lsstSim.LsstSimMapper"));
    }

    void scanForFitsFiles(Path root, FitsFileVisitor visitor) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (fitsPattern.matcher(file.toString()).matches()) {
                    visitor.visit(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private interface FitsFileVisitor {

        public void visit(Path file) throws IOException;
    }

}
