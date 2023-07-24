# FusionDataBench

### quick starter

package structure : 
- task 工作负载查询语句
  - SocialNetworkTask
- script AI脚本
- runner 图相关模型
- Gen 数据生成


#### dataset
import step：
1. download ldbc_data_gen_spark and run it to generator graph data

   ```
   rm -rf out-sf${SF}/graphs/parquet/raw
   tools/run.py \
       --cores $(nproc) \
       --memory ${LDBC_SNB_DATAGEN_MAX_MEM} \
       -- \
       --mode bi \
       --format csv \
       --scale-factor ${SF} \
       --output-dir out-sf${SF}/ \
       --explode-edges \
       --epoch-millis \
       --format-options header=false,quoteAll=true
   ```

2. download imdm-wiki,lfw face dataset, tweet sentiment public dataset, News dataset[TODO]

3. download our ldbc_snb_interactive_impls. it's header and script have been customized.

4. run Gen.java in this project to generator unstructured data

5. export some environment variable[TODO].

   ```
   export NEO4J_CSV_DIR=${LDBC_SNB_DATAGEN_DIR}/out-sf${SF}/graphs/csv/bi/composite-projected-fk/
   ```

6. run load-in-one-step.sh

7. run SocialNetwork.java
