package org.lsst.phosimingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.Header;

/**
 *
 * @author tonyj
 */
class Visit {
    
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    static {
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    private static final Pattern chipIdPattern = Pattern.compile("R(\\d)(\\d)_S(\\d)(\\d)");
    private final boolean gzipped;
    private int r1;
    private int r2;
    private int s1;
    private int s2;
    private double exptime;
    private int filtnm;
    private int visit;
    private String filter;
    private String chipid;
    private double mjd;
    private int pairid;
    private final Path file;

    Visit(Path file) throws IOException {
        this.file = file;
        gzipped = file.toString().endsWith(".gz");
        try (final Fits fits = new Fits(file.toFile())) {
            Header header = Header.readHeader(fits.getStream());
            exptime = header.getDoubleValue("EXPTIME");
            filtnm = header.getIntValue("FILTNM");
            visit = header.getIntValue("OBSID");
            filter = header.getStringValue("FILTER");
            chipid = header.getStringValue("CHIPID");
            Matcher matcher = chipIdPattern.matcher(chipid);
            if (!matcher.matches()) {
                throw new IOException("Invalid chipid: " + chipid);
            }
            r1 = Integer.parseInt(matcher.group(1));
            r2 = Integer.parseInt(matcher.group(2));
            s1 = Integer.parseInt(matcher.group(3));
            s2 = Integer.parseInt(matcher.group(4));
            mjd = header.getDoubleValue("MJD-OBS");
            pairid = header.getIntValue("PAIRID");
        } catch (FitsException ex) {
            throw new IOException("Error reading fits file", ex);
        }
    }

    void createLink(Path output, PhosimIngest.Options options) throws IOException {
        String folder = String.format("eimage/v%d-f%s/E%03d/R%02d/", visit, filter, pairid, getRaft());
        String link = String.format("eimage_%d_R%02d_S%02d_E%03d.fits%s", visit, getRaft(), getSensor(), pairid, gzipped ? ".gz" : "");
        Path path = output.resolve(folder);
        Files.createDirectories(path);
        path = path.resolve(link);
        if (options.update && Files.exists(path)) return;
        if (options.clobber) Files.deleteIfExists(path);
        Files.createSymbolicLink(path, file.toAbsolutePath().normalize());
        if (options.verbose) System.out.printf("%s -> %s\n", path, file.toAbsolutePath().normalize());
    }

    public int getRaft() {
        return 10*r1+r2;
    }

    public int getSensor() {
        return 10*s1+s2;
    }

    public double getExptime() {
        return exptime;
    }

    public int getFiltnm() {
        return filtnm;
    }

    public int getVisit() {
        return visit;
    }

    public String getFilter() {
        return filter;
    }

    public double getMjd() {
        return mjd;
    }

    public int getPairid() {
        return pairid;
    }

    String getRaftName() {
        return r1+","+r2;
    }

    String getSensorName() {
        return s1+","+s2;
    }

    /**
     * Format the time read from the fits header as a string. This method seems
     * insane but is an attempt to reproduce what is done by ingestSimImages.
     * @return The formated string, like: 2025-12-20T01:25:03.589441299
     */
    String getMJDString() {
        // FIXME: Using the current number of leap seconds to correct mjd/tai dates in 2025 seems very dubious
        // current (November 2016) number of leap seconds. 
        int leap_seconds = 36; 
        double unix = (mjd - 40587)*24*60*60 - leap_seconds;
        double fraction = unix - Math.floor(unix);
        long java = 1000 * (long) Math.floor(unix);
        // Note, we do not have nanosecond precission from the data in the fits file, so the
        // last few digits here are going to be arbitrary.
        return String.format("%s.%09d",sdf.format(java),Math.round(fraction*1000000000l));
    }
}
