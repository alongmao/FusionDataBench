package strategy;

import entity.Face;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description: TODO
 * @Author: along
 * @Date: 2023/6/9 15:29
 * @Version 1.0
 */
public class FaceSelectStrategy implements DataSelectStrategy {

    Logger logger = Logger.getLogger(FaceSelectStrategy.class);
//    人脸图片地址
    private final  String FACE_DIR = "/Users/along/Documents/dataset/FaceDataset";

    private final  String[] datasetName = {"lfw","imdb","wiki"};


    private List<Face> faceList;

    private Integer index;


    public FaceSelectStrategy() {
        index = 0;
        this.faceList = new ArrayList<>();
//        initLfw();
        initImdbWiki();
        Collections.shuffle(this.faceList,new Random(42));
    }

    private void initLfw(){
        File file = new File(String.format("%s/lfw",FACE_DIR));
        List<File> imageList = Arrays.stream(file.listFiles()).filter(e -> !e.getName().startsWith(".")).collect(Collectors.toList());

        imageList.forEach(image->{
            try{
                File face = image.listFiles()[0];
                Face faceEntity = new Face();
                faceEntity.setFacePath(face.getAbsolutePath().replace(FACE_DIR,""));
                this.faceList.add(faceEntity);
            }catch (Exception e){
                logger.info("list lfw image fail");
                throw e;
            }
        });
    }

    private void initImdbWiki(){
        try (
                BufferedReader imdbTxt = new BufferedReader(new FileReader(String.format("%s/imdb.txt",FACE_DIR)));
                BufferedReader wikiTxt = new BufferedReader(new FileReader(String.format("%s/wiki.txt",FACE_DIR)));
        ) {
            String line;
            while ((line = imdbTxt.readLine()) != null) {
                String[] paths = line.split(",");
                for (String path : paths) {
                    // Remove leading and trailing whitespaces and add to the list
                    Face face = new Face();
                    face.setFacePath(String.format("/imdb_crop/%s",path.trim()));
                    faceList.add(face);
                }
            }
            while ((line = wikiTxt.readLine()) != null) {
                String[] paths = line.split(",");
                for (String path : paths) {
                    // Remove leading and trailing whitespaces and add to the list
                    Face face = new Face();
                    face.setFacePath(String.format("/wiki_crop/%s",path.trim()));
                    faceList.add(face);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Face select() {
        if(index<this.faceList.size()){
            return faceList.get(index++);
        }
        return null;
    }
}
