package task.fusionDB;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.util.Arrays;
import java.util.List;

public class SF5 extends Tasks{

    public static List<String[]> parameters1 = Arrays.asList( //<personId>, firstname, face
            new String[]{"2199023257333", "Pedro", "/imdb_crop/40/nm0000140_rm3182080_1944-9-25_1997.jpg"},
            new String[]{"2199023279839", "Richard", "/imdb_crop/73/nm0000173_rm2379216640_1967-6-20_2015.jpg"},
            new String[]{"6597069779393", "Rahul", "/imdb_crop/42/nm0000142_rm2473299456_1930-5-31_2009.jpg"},
            new String[]{"10995116284180", "Jorge", "/imdb_crop/79/nm0000179_rm1412470784_1972-12-29_2001.jpg"},
            new String[]{"13194139559065", "Claudio", "/imdb_crop/06/nm0000106_rm774999040_1975-2-22_2007.jpg"},
            new String[]{"17592186063018", "Ernesto", "/imdb_crop/40/nm0000140_rm2871433216_1944-9-25_2007.jpg"},
            new String[]{"21990232568343", "Joseph", "/imdb_crop/07/nm0000107_rm1223996160_1953-12-8_1987.jpg"},
            new String[]{"26388279075264", "John", "/imdb_crop/87/nm0000187_rm487889664_1958-8-16_1996.jpg"},
            new String[]{"30786325596870", "Wei", "/imdb_crop/39/nm0000139_rm2780016640_1972-8-30_2002.jpg"},
            new String[]{"32985348852673", "A.", "/imdb_crop/82/nm0000182_rm296065792_1969-7-24_2006.jpg"})
            ;

    public static final List<String[]> parameters3 = Arrays.asList( //<id>, <face>, city
            new String[]{"2199023257333","/imdb_crop/40/nm0000140_rm3182080_1944-9-25_1997.jpg", "Santiago"},
            new String[]{"2199023279839","/imdb_crop/73/nm0000173_rm2379216640_1967-6-20_2015.jpg", "Geelong"},
            new String[]{"6597069779393","/imdb_crop/42/nm0000142_rm2473299456_1930-5-31_2009.jpg", "Kalimpong"},
            new String[]{"10995116284180","/imdb_crop/79/nm0000179_rm1412470784_1972-12-29_2001.jpg", "Cancún"},
            new String[]{"13194139559065","/imdb_crop/06/nm0000106_rm774999040_1975-2-22_2007.jpg", "Santiago"},
            new String[]{"17592186063018","/imdb_crop/40/nm0000140_rm2871433216_1944-9-25_2007.jpg", "Antón_Lizardo"},
            new String[]{"21990232568343","/imdb_crop/07/nm0000107_rm1223996160_1953-12-8_1987.jpg", "Visakhapatnam"},
            new String[]{"26388279075264","/imdb_crop/87/nm0000187_rm487889664_1958-8-16_1996.jpg", "Tagbilaran"},
            new String[]{"30786325596870","/imdb_crop/39/nm0000139_rm2780016640_1972-8-30_2002.jpg", "Dengta"},
            new String[]{"32985348852673","/imdb_crop/82/nm0000182_rm296065792_1969-7-24_2006.jpg", "Tangerang"})
            ;


    List<String[]> parameters6 = this.parameters11;
    static List<String[]> parameters10 = Arrays.asList( //<commentId>
            new String[]{"2748780008047"},
            new String[]{"3023659978039"},
            new String[]{"3573413399033"},
            new String[]{"3848292222425"},
            new String[]{"3848304349148"},
            new String[]{"4123175215831"},
            new String[]{"4398047262005"},
            new String[]{"4398052624195"},
            new String[]{"4398060538682"},
            new String[]{"4672929265800"});
    static List<String[]> parameters11 = Arrays.asList( //<personId>
            new String[]{"2199023257333"},
            new String[]{"2199023279839"},
            new String[]{"6597069779393"},
            new String[]{"10995116284180"},
            new String[]{"13194139559065"},
            new String[]{"17592186063018"},
            new String[]{"21990232568343"},
            new String[]{"26388279075264"},
            new String[]{"30786325596870"},
            new String[]{"32985348852673"});
    public SF5(Driver driver) {
        super(driver);
    }

    public static void main(String[] args) {
        String uri = "bolt://10.0.82.144:5687";
        String user = "neo4j";
        String pwd = "neo4j";
        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));
        SF5 sf3 = new SF5(driver);
//        sf3.run(1, sf3.task1(SF5.parameters1));
        sf3.run(3, sf3.task3(SF5.parameters3));
//        sf3.run(6, sf3.task6(SF5.parameters11));
        sf3.run(10, sf3.task10(SF5.parameters10));
        sf3.run(11, sf3.task11(SF5.parameters11));
    }
}
