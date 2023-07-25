package strategy;

import entity.Face;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 15:29
 * @Version 1.0
 */
public class FaceSelectStrategy implements DataSelectStrategy, Serializable {

    transient Logger logger = Logger.getLogger(FaceSelectStrategy.class);
    private final String datasetDir = System.getenv("MULTIMODAL_DATASET_DIR");
    //    人脸图片地址
    private final String FACE_DIR = datasetDir + "/FaceDataset";

    private final String[] datasetName = {"lfw", "imdb", "wiki"};

    private List<Face> faceList;

    private Integer index;

    private transient Iterator<Face> faceIt;


    public FaceSelectStrategy(JavaSparkContext sc) {
        index = 0;
        this.faceList = new ArrayList<>();
//        initLfw();
        initImdbWiki(sc);
    }

    private void initLfw() {
        File file = new File(String.format("%s/lfw", FACE_DIR));
        List<File> imageList = Arrays.stream(file.listFiles()).filter(e -> !e.getName().startsWith(".")).collect(Collectors.toList());

        imageList.forEach(image -> {
            try {
                File face = image.listFiles()[0];
                Face faceEntity = new Face();
                faceEntity.setFacePath(face.getAbsolutePath().replace(FACE_DIR, ""));
                this.faceList.add(faceEntity);
            } catch (Exception e) {
                logger.error("list lfw image fail", e);
            }
        });
    }

    private void initImdbWiki() {
        try (
                BufferedReader imdbTxt = new BufferedReader(new FileReader(String.format("%s/imdb.txt", FACE_DIR)));
                BufferedReader wikiTxt = new BufferedReader(new FileReader(String.format("%s/wiki.txt", FACE_DIR)));
        ) {
            String line;
            while ((line = imdbTxt.readLine()) != null) {
                String[] paths = line.split(",");
                for (String path : paths) {
                    // Remove leading and trailing whitespaces and add to the list
                    Face face = new Face();
                    face.setFacePath(String.format("/imdb_crop/%s", path.trim()));
                    faceList.add(face);
                }
            }
            while ((line = wikiTxt.readLine()) != null) {
                String[] paths = line.split(",");
                for (String path : paths) {
                    // Remove leading and trailing whitespaces and add to the list
                    Face face = new Face();
                    face.setFacePath(String.format("/wiki_crop/%s", path.trim()));
                    faceList.add(face);
                }
            }
        } catch (IOException e) {
            logger.error("list imdb-wiki image fail", e);
        }
    }

    private void initImdbWiki(JavaSparkContext sc) {
        JavaRDD<Face> imdbRdd = sc.textFile(String.format("%s/imdb.txt", FACE_DIR)).flatMap(line -> {
            List<Face> faceList = new ArrayList<>();
            String[] paths = line.split(",");
            for (String path : paths) {
                Face face = new Face();
                face.setFacePath(String.format("/imdb_crop/%s", path.trim()));
                faceList.add(face);
            }
            return faceList.iterator();
        });


        JavaRDD<Face> wikiRdd = sc.textFile(String.format("%s/wiki.txt", FACE_DIR)).flatMap(line -> {
            List<Face> faceList = new ArrayList<>();
            String[] paths = line.split(",");
            for (String path : paths) {
                Face face = new Face();
                face.setFacePath(String.format("/wiki_crop/%s", path.trim()));
                faceList.add(face);
            }
            return faceList.iterator();
        });

        long size = imdbRdd.count() + wikiRdd.count();

        this.faceIt = imdbRdd.union(wikiRdd).toLocalIterator();
    }

    @Override
    public Face select() {
        if (faceIt.hasNext()) {
            return faceIt.next();
        }
        logger.info("select Fave Image fail");
        return null;
    }
}
