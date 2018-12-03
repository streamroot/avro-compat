package io.streamroot.avro.tool;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GenDefaultTool implements Tool {

    @Override
    public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
        OptionParser p = new OptionParser();

        OptionSpec<String> file =
                p.accepts("schema-file", "Schema File")
                        .withOptionalArg()
                        .ofType(String.class);

        OptionSpec<String> inSchema =
                p.accepts("schema", "Schema")
                        .withOptionalArg()
                        .ofType(String.class);

        OptionSpec<String> nested =
                p.accepts("nested", "Path to a nested record schema")
                        .withOptionalArg()
                        .ofType(String.class);

        OptionSpec<Void> noPrettyOption = p
                .accepts("no-pretty", "Turns off pretty printing");

        OptionSet opts = p.parse(args.toArray(new String[0]));
        if (opts.nonOptionArguments().size() != 1) {
            err.println("Usage: " +
                    "(--schema-file <file> | --schema <schema>) " +
                    "[--nested <path>]" +
                    " out (filename or '-' for stdout) " +
                    "[--no-pretty]");
            p.printHelpOn(err);
            return 1;
        }
        args = (List<String>) opts.nonOptionArguments();

        String schemaStr = inSchema.value(opts);
        String schemaFile = file.value(opts);
        String nestedPath = nested.value(opts);
        Boolean noPretty = opts.has(noPrettyOption);
        if (schemaStr == null && schemaFile == null) {
            err.println("Need input schema (--schema-file) or (--schema)");
            p.printHelpOn(err);
            return 1;
        }

        Schema schema = (schemaFile != null)
                ? Util.parseSchemaFromFS(schemaFile)
                : new Schema.Parser().parse(schemaStr);
        if (nestedPath != null) {
            schema = nestedSchema(schema, nestedPath);
        }

        try(BufferedOutputStream bos = Util.fileOrStdout(args.get(0), out)) {
            DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, bos, !noPretty);
            writer.write(genDefault(schema), encoder);
            encoder.flush();
        }

        return 0;
    }

    private static Object genDefault(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    Object value = GenericData.get().getDefaultValue(field);
                    record.put(field.name(), value);
                }
                return record;
            default: throw new RuntimeException("Handles only RECORD schemas, got: " + schema);
        }
    }

    private static Schema nestedSchema(Schema schema, String path) {
        Schema current = schema;
        String[] fields = path.substring(1).split("\\.");

        int i = 0;
        for (; i<fields.length; i++) {
            if (fields[i].endsWith("[]")) {
                String field = fields[i].substring(0, fields[i].length() - 2);
                Schema next = current.getField(field).schema();
                if (!next.getType().equals(Schema.Type.ARRAY))
                    throw new RuntimeException("Not an array: " + next);
                current = next.getElementType();

            } else if (fields[i].endsWith("{}")) {
                String field = fields[i].substring(0, fields[i].length() - 2);
                Schema next = current.getField(field).schema();
                if (!next.getType().equals(Schema.Type.MAP))
                    throw new RuntimeException("Not a map: " + next);
                current = next.getValueType();

            } else if (fields[i].endsWith("?")) {
                String field = fields[i].substring(0, fields[i].length() - 1);
                Schema next = current.getField(field).schema();
                if (!next.getType().equals(Schema.Type.UNION))
                    throw new RuntimeException("Not a union: " + next);
                if (!(next.getTypes().size() == 2 && next.getTypes().get(0).getType().equals(Schema.Type.NULL)))
                    throw new RuntimeException("Invalid optional union: " + next);
                Schema s = next.getTypes().get(1);
                switch (s.getType()) {
                    case RECORD:
                        current = s;
                        break;
                    case MAP:
                        current = s.getValueType();
                        break;
                    case ARRAY:
                        current = s.getElementType();
                        break;
                    default:
                        throw new RuntimeException("Invalid optional union of RECORD|MAP|ARRAY: " + next);
                }

            } else {
                Objects.requireNonNull(current.getField(fields[i]));
                current = current.getField(fields[i]).schema();
            }
        }

        return current;
    }

    @Override
    public String getName() {
        return "default";
    }

    @Override
    public String getShortDescription() {
        return "Generates a json record with a schema's default values";
    }
}
