import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class GeoGps extends UDF {
    {
        // try{
        //     BufferedReader oCityMappingreader = new BufferedReader(new FileReader(new File(CITY_MAPPING_FILE)));
        //     String sLine = null;
        //     while ((sLine = oCityMappingreader.readLine()) != null) {
        //         String sFields[] = sLine.split(",");
        //         String sShpProvince = sFields[0].toString().toLowerCase().trim();
        //         String sShpCity = sFields[1].toString().toLowerCase().trim();
        //         String sDimCity = sFields[2].toString().toLowerCase().trim();
        //         m_oCityMapping.put(sShpProvince.concat("_").concat(sShpCity), sDimCity);
        //     }
        //     oCityMappingreader.close();
        // } catch (Exception e) {
        //     System.out.println(e.getMessage());
        // }

        try{
            oShpDataStore = new ShapefileDataStore(new File(SHP_FILE).toURI().toURL());
            //oShpDataStore.setCharset(Charset.forName("GBK"));
            String sTypeName = oShpDataStore.getTypeNames()[0];
            FeatureSource<SimpleFeatureType, SimpleFeature> oFeatureSource = (FeatureSource<SimpleFeatureType, SimpleFeature>) oShpDataStore.getFeatureSource(sTypeName);
            FeatureCollection<SimpleFeatureType, SimpleFeature> oResult = oFeatureSource.getFeatures();
            FeatureIterator<SimpleFeature> itertor = oResult.features();

            while(itertor.hasNext()){
                SimpleFeature oFeature = itertor.next();
                Geometry oGeom = (Geometry) oFeature.getDefaultGeometry();
                String sCountry = oFeature.getAttribute("NAME_0") == null ? "" : oFeature.getAttribute("NAME_0").toString().toLowerCase().trim();
                String sProvince = oFeature.getAttribute("NAME_1") == null ? "" : oFeature.getAttribute("NAME_1").toString().toLowerCase().trim();
                String sCity = oFeature.getAttribute("NAME_2") == null ? "" : oFeature.getAttribute("NAME_2").toString().toLowerCase().trim();
                String sCounty = oFeature.getAttribute("NAME_3") == null ? "" : oFeature.getAttribute("NAME_3").toString().toLowerCase().trim();

                CityPolygon oCityPolygon = new CityPolygon();
                oCityPolygon.oGeo = oGeom;
                oCityPolygon.sCountry = sCountry;
                oCityPolygon.sProvince = sProvince;
                oCityPolygon.sCity = sCity;
                //oCityPolygon.sCity = m_oCityMapping.containsKey(sProvince.concat("_").concat(sCity)) ? m_oCityMapping.get(sProvince.concat("_").concat(sCity)) : sCity;
                oCityPolygon.sCounty = sCounty;

                l_oCityPolygon.add(oCityPolygon);
            }
            itertor.close();
            UDF_STATUS_OK = true;
        } catch (Exception e) {
            UDF_STATUS_OK = false;
        }
    }

    public static String evaluate(double dLantitude, double dLongitude, String sQueryKey) {
        String sRet = DEFAULT_RETURN_STRING;

        if (sQueryKey == null || !UDF_STATUS_OK) {
            return sRet;
        }

        sQueryKey = sQueryKey.toLowerCase().trim();
        boolean bIsQueryValid = false;

        for (int i = 0; i < FIELD_KEYS.length; i ++) {
            if (FIELD_KEYS[i].equals(sQueryKey)) {
                bIsQueryValid = true;
                break;
            }
        }

        if (!bIsQueryValid) {
            return sRet;
        }

        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Point pTestPoint = geometryFactory.createPoint(new Coordinate(dLantitude, dLongitude));

        for (int i = 0; i < l_oCityPolygon.size(); i ++) {
            CityPolygon oCurCityPolygon = l_oCityPolygon.get(i);
            if (oCurCityPolygon.oGeo != null && oCurCityPolygon.oGeo.contains(pTestPoint)) {
                if (sQueryKey.equals(FIELD_KEYS[FIELD_COUNTRY])) {
                    sRet = oCurCityPolygon.sCountry;
                } else if (sQueryKey.equals(FIELD_KEYS[FIELD_PROV])) {
                    sRet = oCurCityPolygon.sProvince;
                } else if (sQueryKey.equals(FIELD_KEYS[FIELD_CITY])) {
                    sRet = oCurCityPolygon.sCity;
                } else if (sQueryKey.equals(FIELD_KEYS[FIELD_COUNTY])) {
                    sRet = oCurCityPolygon.sCounty;
                }
                break;
            }
        }
        return sRet;
    }

    /*
     shp file downloaded from http://www.gadm.org/.
     link: http://data.biogeo.ucdavis.edu/data/gadm2/shp/CHN_adm.zip
     */

    protected static final String SHP_FILE = "/tmp/CHN_adm3.shp";
    //protected static final String CITY_MAPPING_FILE = "/home/deploy/bihive/global/CHN_adm3.map";
    protected static boolean UDF_STATUS_OK = true;
    protected static final String DEFAULT_RETURN_STRING = "UNKNOWN";

    protected static ShapefileDataStore oShpDataStore = null;
    protected static List<CityPolygon> l_oCityPolygon = new ArrayList<CityPolygon>();
    //protected static Map<String, String> m_oCityMapping = new HashMap<String, String>();

    protected static final String FIELD_KEYS[] = {
        "country",    //country name
        "prov",    //province name
        "city",        //city name
        "county"    //county name
    };

    protected static final int FIELD_COUNTRY = 0;
    protected static final int FIELD_PROV = 1;
    protected static final int FIELD_CITY = 2;
    protected static final int FIELD_COUNTY = 3;

    protected class CityPolygon {
        protected Geometry oGeo = null;
        protected String sCountry = null;
        protected String sProvince = null;
        protected String sCity = null;
        protected String sCounty = null;
    }
}
