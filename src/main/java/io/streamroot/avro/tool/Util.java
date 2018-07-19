package io.streamroot.avro.tool;

import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.Deflater;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;

import joptsimple.OptionSet;
import joptsimple.OptionParser;
import joptsimple.OptionSpec;


/**
 * Static utility methods for tools.
 */
class Util {
    /**
     * Returns stdin if filename is "-", else opens the File in the owning filesystem
     * and returns an InputStream for it.
     * Relative paths will be opened in the default filesystem.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    static BufferedInputStream fileOrStdin(String filename, InputStream stdin)
            throws IOException {
        return new BufferedInputStream(filename.equals("-")
                ? stdin
                : openFromFS(filename));
    }

    /**
     * Returns stdout if filename is "-", else opens the file from the owning filesystem
     * and returns an OutputStream for it.
     * Relative paths will be opened in the default filesystem.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    static BufferedOutputStream fileOrStdout(String filename, OutputStream stdout)
            throws IOException {
        return new BufferedOutputStream(filename.equals("-")
                ? stdout
                : createFromFS(filename));
    }

    /**
     * Returns an InputStream for the file using the owning filesystem,
     * or the default if none is given.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    static InputStream openFromFS(String filename)
            throws IOException {
        return new FileInputStream(filename);
    }

    /**
     * Returns an InputStream for the file using the owning filesystem,
     * or the default if none is given.
     *
     * @param filename The filename to be opened
     * @throws IOException
     */
    static InputStream openFromFS(Path filename)
            throws IOException {
        return new FileInputStream(filename.toFile());
    }

    /**
     * Opens the file for writing in the owning filesystem,
     * or the default if none is given.
     *
     * @param filename The filename to be opened.
     * @return An OutputStream to the specified file.
     * @throws IOException
     */
    static OutputStream createFromFS(String filename)
            throws IOException {
        Path p = Paths.get(filename);
        return new BufferedOutputStream(new FileOutputStream(p.toFile()));
    }

    /**
     * Closes the inputstream created from {@link Util.fileOrStdin}
     * unless it is System.in.
     *
     * @param in The inputstream to be closed.
     */
    static void close(InputStream in) {
        if (!System.in.equals(in)) {
            try {
                in.close();
            } catch (IOException e) {
                System.err.println("could not close InputStream " + in.toString());
            }
        }
    }

    /**
     * Closes the outputstream created from {@link Util.fileOrStdin}
     * unless it is System.out.
     *
     * @param out The outputStream to be closed.
     */
    static void close(OutputStream out) {
        if (!System.out.equals(out)) {
            try {
                out.close();
            } catch (IOException e) {
                System.err.println("could not close OutputStream " + out.toString());
            }
        }
    }

    /**
     * Parses a schema from the specified file.
     *
     * @param filename The file name to parse
     * @return The parsed schema
     * @throws IOException
     */
    static Schema parseSchemaFromFS(String filename) throws IOException {
        InputStream stream = openFromFS(filename);
        try {
            return new Schema.Parser().parse(stream);
        } finally {
            close(stream);
        }
    }

    /**
     * Converts a String JSON object into a generic datum.
     * <p>
     * This is inefficient (creates extra objects), so should be used
     * sparingly.
     */
    static Object jsonToGenericDatum(Schema schema, String jsonData)
            throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object datum = reader.read(null,
                DecoderFactory.get().jsonDecoder(schema, jsonData));
        return datum;
    }

    /**
     * Reads and returns the first datum in a data file.
     */
    static Object datumFromFile(Schema schema, String file) throws IOException {
        DataFileReader<Object> in =
                new DataFileReader<>(new File(file),
                        new GenericDatumReader<>(schema));
        try {
            return in.next();
        } finally {
            in.close();
        }
    }

    static OptionSpec<String> compressionCodecOption(OptionParser optParser) {
        return optParser
                .accepts("codec", "Compression codec")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("null");
    }

    static OptionSpec<Integer> compressionLevelOption(OptionParser optParser) {
        return optParser
                .accepts("level", "Compression level (only applies to deflate and xz)")
                .withRequiredArg()
                .ofType(Integer.class)
                .defaultsTo(Deflater.DEFAULT_COMPRESSION);
    }

    static CodecFactory codecFactory(OptionSet opts, OptionSpec<String> codec, OptionSpec<Integer> level) {
        return codecFactory(opts, codec, level, DEFLATE_CODEC);
    }

    static CodecFactory codecFactory(OptionSet opts, OptionSpec<String> codec, OptionSpec<Integer> level, String defaultCodec) {
        String codecName = opts.hasArgument(codec)
                ? codec.value(opts)
                : defaultCodec;
        if (codecName.equals(DEFLATE_CODEC)) {
            return CodecFactory.deflateCodec(level.value(opts));
        } else if (codecName.equals(DataFileConstants.XZ_CODEC)) {
            return CodecFactory.xzCodec(level.value(opts));
        } else {
            return CodecFactory.fromString(codec.value(opts));
        }
    }
}