package io.streamroot.avro.tool;

import com.hotels.avro.compatibility.Compatibility;
import com.hotels.avro.compatibility.CompatibilityCheckResult;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

public class CheckerTool implements Tool {

    static String asJq(String jsonPointer) {
        // formats JSON pointer as jq query
        return jsonPointer.replaceAll("/([0-9]+)", "[$1]").replaceAll("/", ".");
    }

    static String compat(Compatibility.CheckType compat) {
        switch (compat) {
            case CAN_READ:       return "Backward";
            case CAN_BE_READ_BY: return "Forward";
            default:             throw new UnsupportedOperationException(compat.toString());
        }
    }

    static String message(CompatibilityCheckResult res) {
        if (res.isCompatible()) {
            return String.format("Compatibility type '%s' holds between schemas.", compat(res.getCompatibility()));
        } else {
            StringBuilder message = new StringBuilder();
            message.append(String.format("Compatibility type '%s' does not hold between schemas, incompatibilities: [\n",
                    compat(res.getCompatibility())));
            boolean first = true;
            for (SchemaCompatibility.Incompatibility incompatibility : res.getResult().getIncompatibilities()) {
                if (first) {
                    first = false;
                } else {
                    message.append(",\n");
                }
                message.append(String.format("'%s: %s' at '%s'",
                        incompatibility.getType(),
                        incompatibility.getMessage(),
                        asJq(incompatibility.getLocation())));
            }
            message.append("]");
            return message.toString();
        }
    }

    @Override
    public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
        OptionParser optionParser = new OptionParser();

        OptionSpec<String> oldSchemaFileOption = optionParser
                .accepts("old-schema", "File containing old schema")
                .withRequiredArg()
                .ofType(String.class);

        OptionSpec<String> newSchemaFileOption = optionParser
                .accepts("new-schema", "File containing new schema")
                .withRequiredArg()
                .ofType(String.class);

        OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
        List<OptionSpec<?>> nargs = optionSet.specs();

        if (nargs.size() != 2) {
            err.println("check --new-schema <file> --old-schema <file>");
            err.println("   " + getShortDescription());
            optionParser.printHelpOn(err);
            return 1;
        }

        Schema oldSchema = Util.parseSchemaFromFS(oldSchemaFileOption.value(optionSet));
        Schema newSchema = Util.parseSchemaFromFS(newSchemaFileOption.value(optionSet));

        CompatibilityCheckResult backward = Compatibility.checkThat(newSchema).canRead(oldSchema);
        out.append(message(backward));
        out.println();

        CompatibilityCheckResult forward = Compatibility.checkThat(newSchema).canBeReadBy(oldSchema);
        out.append(message(forward));
        out.println();

        out.flush();
        return backward.isCompatible() && forward.isCompatible() ? 0 : 1;
    }

    @Override
    public String getName() {
        return "check";
    }

    @Override
    public String getShortDescription() {
        return "Checks compatibilities between Avro schemas";
    }
}
