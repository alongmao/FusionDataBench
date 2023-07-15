package strategy;

import entity.Face;
import org.apache.log4j.Logger;

import java.io.File;
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
    private final  String FACE_DIR = "/Users/along/Documents/dataset/FaceDataset/lfw";


    private List<Face> faceList;

    private Integer index;


    public FaceSelectStrategy() {
        index = 0;
        this.faceList = new ArrayList<>();

        File file = new File(FACE_DIR);
        List<File> imageList = Arrays.stream(file.listFiles()).filter(e -> !e.getName().startsWith(".")).collect(Collectors.toList());

        imageList.forEach(image->{
            try{
                File face = image.listFiles()[0];
                Face faceEntity = new Face();
                faceEntity.setFacePath(face.getAbsolutePath());
                this.faceList.add(faceEntity);
            }catch (Exception e){
                logger.info("list image fail");
                throw e;
            }
        });

        Collections.shuffle(this.faceList,new Random(42));
    }

    @Override
    public Face select() {
        if(index<this.faceList.size()){
            return faceList.get(index++);
        }
        return null;
    }
}
