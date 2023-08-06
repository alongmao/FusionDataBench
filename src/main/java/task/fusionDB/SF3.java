package task.fusionDB;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.util.Arrays;
import java.util.List;

public class SF3 extends Tasks {


    public static List<String[]> parameters1 = Arrays.asList( //<personId>, firstname, face
            new String[]{"2199023256759", "Atef", "/imdb_crop/13/nm0000213_rm2469826048_1971-10-29_1988.jpg"},
            new String[]{"2199023284334", "Muhammad", "/imdb_crop/59/nm0000159_rm1470797568_1964-12-8_2002.jpg"},
            new String[]{"6597069780697", "Jose", "/imdb_crop/00/nm0000200_rm1718005760_1955-5-17_2011.jpg"},
            new String[]{"10995116283164", "Pedro", "/imdb_crop/01/nm0000201_rm1206687744_1958-4-29_1997.jpg"},
            new String[]{"13194139563762", "Ahmad", "/imdb_crop/23/nm0000123_rm887288832_1961-5-6_2015.jpg"},
            new String[]{"17592186064849", "Arjun", "/imdb_crop/01/nm0000201_rm1872018432_1958-4-29_1983.jpg"},
            new String[]{"21990232567793", "A.", "/imdb_crop/59/nm0000159_rm63102208_1964-12-8_1987.jpg"},
            new String[]{"26388279076537", "Juliana", "/imdb_crop/13/nm0000213_rm4041770240_1971-10-29_2011.jpg"},
            new String[]{"30786325601710", "Aditya", "/imdb_crop/59/nm0000159_rm2077077504_1964-12-8_2009.jpg"},
            new String[]{"32985348858238", "Julia", "/imdb_crop/13/nm0000213_rm1317513472_1971-10-29_2012.jpg"})
            ;

    public static final List<String[]> parameters3 = Arrays.asList( //<id>, <face>, city
            new String[]{"2199023256759", "/imdb_crop/13/nm0000213_rm2469826048_1971-10-29_1988.jpg", "Bilbeis"},
            new String[]{"2199023284334", "/imdb_crop/59/nm0000159_rm1470797568_1964-12-8_2002.jpg", "Tangerang"},
            new String[]{"6597069780697", "/imdb_crop/00/nm0000200_rm1718005760_1955-5-17_2011.jpg", "Naga"},
            new String[]{"10995116283164", "/imdb_crop/01/nm0000201_rm1206687744_1958-4-29_1997.jpg", "Évora"},
            new String[]{"13194139563762", "/imdb_crop/23/nm0000123_rm887288832_1961-5-6_2015.jpg", "Giza"},
            new String[]{"17592186064849", "/imdb_crop/01/nm0000201_rm1872018432_1958-4-29_1983.jpg", "Moga"},
            new String[]{"21990232567793", "/imdb_crop/59/nm0000159_rm63102208_1964-12-8_1987.jpg", "Jiaganj_Azimganj"},
            new String[]{"26388279076537", "/imdb_crop/13/nm0000213_rm4041770240_1971-10-29_2011.jpg", "Santos"},
            new String[]{"30786325601710", "/imdb_crop/59/nm0000159_rm2077077504_1964-12-8_2009.jpg", "Darbhanga"},
            new String[]{"32985348858238","/imdb_crop/13/nm0000213_rm1317513472_1971-10-29_2012.jpg", "Monze" }
    );

    static List<String[]> parameters6 = SF3.parameters11;
    static List<String[]> parameters10 = Arrays.asList( //<commentId>
            new String[]{"2748780256669"},
            new String[]{"3023660577642"},
            new String[]{"3573412989443"},
            new String[]{"3848292160278"},
            new String[]{"3848304705424"},
            new String[]{"4123175679055"},
            new String[]{"4398047317877"},
            new String[]{"4398052981091"},
            new String[]{"4398061526822"},
            new String[]{"4672929235868"});
    static List<String[]> parameters11 = Arrays.asList( //<personId>
            new String[]{"2199023256759"},
            new String[]{"2199023284334"},
            new String[]{"6597069780697"},
            new String[]{"10995116283164"},
            new String[]{"13194139563762"},
            new String[]{"17592186064849"},
            new String[]{"21990232567793"},
            new String[]{"26388279076537"},
            new String[]{"30786325601710"},
            new String[]{"32985348858238"});

    public SF3(Driver driver) {
        super(driver);
    }

    public static void main(String[] args) {
        String uri = "bolt://10.0.82.144:8687";
        String user = "neo4j";
        String pwd = "neo4j";
        Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(user, pwd));
        SF3 sf3 = new SF3(driver);
//        sf3.run(1, sf3.task1(SF3.parameters1));
        sf3.run(3, sf3.task3(SF3.parameters3));
//        sf3.run(6, sf3.task6(SF3.parameters11));
        sf3.run(10, sf3.task10(SF3.parameters10));
        sf3.run(11, sf3.task11(SF3.parameters11));
    }
}
