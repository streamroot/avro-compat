package io.streamroot.avro.tool;

import org.junit.Test;

import static io.streamroot.avro.tool.CheckerTool.asJq;
import static org.junit.Assert.*;

public class CheckerToolTest {

    @Test
    public void testJsonPathFormatting() {

        assertEquals(".fields[42].type.fields[2].type.fields[0].type.fields[2].type",
                asJq("/fields/42/type/fields/2/type/fields/0/type/fields/2/type"));

        assertEquals(".fields[1].type[2]",
                asJq("/fields/1/type/2"));
    }

}