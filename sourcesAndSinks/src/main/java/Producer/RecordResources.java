package Producer;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class RecordResources {

    public static List getInitialiseData(int numberofrecordrequests, int configurationSwitch) {
        List<String> fnList = Arrays.asList(
                "Olivia", "Sophia", "Amelia", "Lily", "Emily", "Ava", "Isla", "Isabella", "Mia", "Isabelle",
                "Ella", "Poppy", "Freya", "Grace", "Sophie", "Evie", "Charlotte", "Aria", "Evelyn", "Phoebe",
                "Chloe", "Daisy", "Alice", "Ivy", "Darcie", "Sienna", "Harper", "Hannah", "Ruby", "Scarlett",
                "Maya", "Jessica", "Layla", "Matilda", "Willow", "Eva", "Emma", "Erin", "Florence", "Molly",
                "Rosie", "Millie", "Emilia", "Mila", "Esme", "Elsie", "Maisie", "Ellie", "Lucy", "Thea",
                "Zoe", "Nur", "Imogen", "Luna0", "Lola", "Zara", "Maryam", "Bella", "Holly", "Annabelle",
                "Eleanor", "Eliza", "Amber", "Abigail", "Lyla", "Penelope", "Niamh", "Madison", "Violet", "Fatima",
                "Georgia", "Sarah", "Elizabeth", "Amelie", "Jasmine", "Harriet", "Rose", "Lexi", "Nancy", "Anna",
                "Amy", "Leah", "Summer", "Muhammad", "Oliver", "Harry", "Jack", "George", "Noah", "Leo",
                "Jacob", "Oscar", "Charlie", "Jackson", "William", "Joshua", "Ethan", "James", "Freddie", "Alfie"
        );
        List<String> snList = Arrays.asList(
                "Smith", "Jones", "Taylor", "Williams", "Brown", "Davies", "Evans", "Wilson", "Thomas", "Roberts",
                "Johnson", "Lewis", "Walker", "Robinson", "Wood", "Thompson", "White", "Watson", "Jackson", "Wright",
                "Green", "Harris", "Cooper", "King", "Lee", "Martin", "Clarke", "James", "Morgan", "Hughes",
                "Edwards", "Hill", "Moore", "Clark", "Harrison", "Scott", "Young", "Morris", "Hall", "Ward",
                "Turner", "Carter", "Phillips", "Mitchell", "Patel", "Adams", "Campbell", "Anderson", "Allen", "Cook",
                "Bailey", "Parker", "Miller", "Davis", "Murphy", "Price", "Bell", "Baker", "Griffiths", "Kelly",
                "Simpson", "Marshall", "Collins", "Bennett", "Cox", "Richardson", "Fox", "Gray", "Rose", "Chapman",
                "Hunt", "Robertson", "Shaw", "Reynolds", "Lloyd", "Ellis", "Richards", "Russell", "Wilkinson", "Khan",
                "Graham", "Stewart", "Reid", "Murray", "Powell", "Palmer", "Holmes", "Rogers", "Stevens", "Walsh",
                "Hunter", "Thomson", "Matthews", "Ross", "Owen", "Mason", "Knight", "Kennedy", "Butler", "Saunders"
        );
        List<Row> rows = new ArrayList<>();
        SimpleDateFormat ft = new SimpleDateFormat("EyyMMdd'-'HH:mm:ss:SSS"); //azzz");


        Random rand = new Random();
        if (configurationSwitch != 1) {
            numberofrecordrequests = rand.nextInt(numberofrecordrequests) + 1;
        }
        for (int i = 0; i < numberofrecordrequests; i++) {
            int randomIndexfn = rand.nextInt(fnList.size());
            int randomIndexsn = rand.nextInt(snList.size());
            String randomElementfn = fnList.get(randomIndexfn);
            String randomElementsn = snList.get(randomIndexsn);
            String randomname = randomElementsn + "_" + randomElementfn;
            if (configurationSwitch == 1) {
                for (int ii = 0; ii < 3; ii++) {
                    rows.add(RowFactory.create(i + 1, randomname, "SocialSurveyID_" + (ii + 1), 0));
                }
            } else {
                Date dNow = new Date();
                int randomWeeklySpend = rand.nextInt(1000); //Rounded (int) Pounds Sterling Return...
                rows.add(RowFactory.create(ft.format(dNow) + "_" + i, randomname, randomWeeklySpend));
            }

        }
        System.out.println("RecordResources Component Production Output: " + rows);
        return rows;
    }
}
