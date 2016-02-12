/**
 * Created by Andrii_Krasnolob on 2/1/2016.
 */
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/*import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;*/
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class UserAgentUDTF extends GenericUDTF{

    Object forwardObj[] = new Object[2];

    @Override
    public void process(Object[] objects) throws HiveException {

        PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        final String userAgent = stringOI.getPrimitiveJavaObject(objects[0]).toString();

        ArrayList<Object[]> results = processInputRecord(userAgent);

        Iterator<Object[]> it = results.iterator();

        while (it.hasNext()){
            Object[] r = it.next();
            forward(r);
        }
    }

    private ArrayList<Object[]> processInputRecord(String inputValue) {
        ArrayList<Object[]> result = new ArrayList<Object[]>();
        if (inputValue == null || inputValue.isEmpty()) {
            return result;
        }
        /*
        // Get an UserAgentStringParser and analyze the requesting client
        UserAgentStringParser parser = UADetectorServiceFactory.getOnlineUpdatingParser();
        ReadableUserAgent agent = parser.parse(inputValue);
        result.add(new Object[] {
                agent.getType().getName(),
                agent.getName(),
                agent.getOperatingSystem().getName(),
                agent.getDeviceCategory().getName() } );
                */
        UserAgent userAgent = UserAgent.parseUserAgentString(inputValue);
        result.add(new Object[] {
                userAgent.getBrowser().getBrowserType().getName(),
                userAgent.getBrowser().getGroup().getName(),
                userAgent.getOperatingSystem().getName(),
                userAgent.getOperatingSystem().getDeviceType().getName() } );
        return result;
    }

    @Override
    public void close() throws HiveException {

    }


    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        if (argOIs.getAllStructFieldRefs().size() != 1){
            throw new UDFArgumentException("UserAgentUDTF() takes exactly one argument");
        }
        // output inspectors -- an object with two fields!
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
//https://udger.com/resources/online-parser
        fieldNames.add("UA type");
        fieldNames.add("UA name");
        fieldNames.add("OS name");
        fieldNames.add("Device");

        for (int i = 0; i < fieldNames.size(); ++i){
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }
}
