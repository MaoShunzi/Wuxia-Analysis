一、编译方法

源码包括Wuxia_preprocess、Wuxia_getFeature、Wuxia_getMap、Wuxia_pagerank、Wuxia_LPA五个部分，使用IntelliJ IDEA分别编译并打包成五个jar包。所有的包都需要依赖hadoop的hadoop-common- 2.7.1.jar、hadoop-mapreduce-client-core-2.7.1.jar和mapreduce/ lib下的包，其中Wuxia_preprocess需要额外依赖ansj_seg-5.1.6.jar、tree_split -1.5.jar、nlp-lang-1.7.7.jar。

二、运行方法

1. hadoop jar Wuxia_preprocess.jar novels preprocess
2. hadoop jar Wuxia_getFeature.jar preprocess feature
3. hadoop jar Wuxia_getMap.jar feature map
4. hadoop jar Wuxia_pagerank.jar map result
5. hadoop jar Wuxia_LPA.jar map LPA
