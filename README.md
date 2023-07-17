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
2. download lfw and tweet sentiment public dataset[TODO]
3. run Gen.java in this project to generator unstructured data
4. export some environment variable[TODO].
5. replace load-in-one-step.sh of LDBC with our script[TODO]
6. run load-in-one-step.sh
