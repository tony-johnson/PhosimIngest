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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import nom.tam.fits.FitsFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

/**
 * Main routine of the PhosimIngest program.
 *
 * @author tonyj
 */
public class PhosimIngest {

    private static final Pattern fitsPattern = Pattern.compile(".*\\.fits(\\.gz)?");
    private final Options options = new Options();
    ExecutorService executor;
    CompletionService<Visit> ecs;

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

        @Option(name = "-t", usage = "number of threads to use for fits file processing")
        int nThreads = 32;

        @Option(name = "-s", usage = "Suppress commit after each visit")
        boolean suppressCommit = false;
    }

    public static void main(String[] args) throws IOException, SQLException, InterruptedException, ExecutionException {
        new PhosimIngest().doMain(args);
    }

    private void doMain(String[] args) throws IOException, SQLException, InterruptedException, ExecutionException {
        CmdLineParser parser = new CmdLineParser(options);
        executor = Executors.newFixedThreadPool(options.nThreads);
        ecs = new ExecutorCompletionService<>(executor);
        try {
            parser.parseArgument(args);
            FitsFactory.setUseHierarch(true);
            Path input = options.in.toPath();
            Path output = options.out.toPath();
            Files.createDirectories(output);
            createMapper(output);
            int nFiles = scanForFitsFiles(input, (file) -> scheduleFitsFile(file, output));
            try (Registry registry = new Registry(output, options)) {
                for (int n = 0; n < nFiles; n++) {
                    Future<Visit> future = ecs.take();
                    registry.addVisit(future.get());
                }
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
        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

    }

    private void scheduleFitsFile(Path file, Path output) {
        ecs.submit(() -> handleFitsFile(file, output));
    }

    private Visit handleFitsFile(Path file, Path output) throws IOException {

        Visit visit = new Visit(file);
        visit.createLink(output, options);
        return visit;
    }

    private void createMapper(Path output) throws IOException {
        Files.write(output.resolve("_mapper"), Collections.singletonList("lsst.obs.lsstSim.LsstSimMapper"));
    }

    int scanForFitsFiles(Path root, FitsFileVisitor visitor) throws IOException {
        AtomicInteger n = new AtomicInteger(0);
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (fitsPattern.matcher(file.toString()).matches()) {
                    visitor.visit(file);
                    n.incrementAndGet();
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return n.get();
    }

    private interface FitsFileVisitor {

        public void visit(Path file) throws IOException;
    }

}
