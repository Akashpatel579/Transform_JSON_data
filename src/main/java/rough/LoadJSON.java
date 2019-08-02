package rough;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.collection.Seq;

import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

public class LoadJSON {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        JSONParser parser = new JSONParser();

        try {

            Object obj = parser.parse(new FileReader(
                    "/Users/akashpatel/IdeaProjects/Transform_JSON_data/src/main/resources/sample.json"));

            JSONObject jsonObject = (JSONObject) obj;

            String name = (String) jsonObject.get("Name");
//            String author = (String) jsonObject.get("Author");
//            JSONArray companyList = (JSONArray) jsonObject.get("Company List");

            System.out.println("Name: " + name);
//            System.out.println("Author: " + author);
//            System.out.println("\nCompany List:");

            // getting phoneNumbers
            JSONArray jsonArray = (JSONArray) jsonObject.get("table_mapping");

            // iterating phoneNumbers
            Iterator itr2 = jsonArray.iterator();

            while (itr2.hasNext())
            {
                Iterator itr1 = ((Map<Object, Object>) itr2.next()).entrySet().iterator();
                while (itr1.hasNext()) {
                    Map.Entry pair = (Map.Entry) itr1.next();
                    System.out.println(pair.getKey() + " : " + pair.getValue());
                }
            }


            List<String> ls = (List<String>) jsonArray.stream()
                    .map(x -> ((JSONObject) x).get("source_destination")).collect(Collectors.toList());

            List<String> supplierNames = Arrays.asList("sup1", "sup2", "sup3");

            System.out.println(supplierNames);
            System.out.println((supplierNames));




        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
