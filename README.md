һ�����뷽��
Դ�����Wuxia_preprocess��Wuxia_getFeature��Wuxia_getMap��Wuxia_pagerank��Wuxia_LPA������֣�ʹ��IntelliJ IDEA�ֱ���벢��������jar�������еİ�����Ҫ����hadoop��hadoop-common- 2.7.1.jar��hadoop-mapreduce-client-core-2.7.1.jar��mapreduce/ lib�µİ�������Wuxia_preprocess��Ҫ��������ansj_seg-5.1.6.jar��tree_split -1.5.jar��nlp-lang-1.7.7.jar��
�������з���
1. hadoop jar Wuxia_preprocess.jar novels preprocess
2. hadoop jar Wuxia_getFeature.jar preprocess feature
3. hadoop jar Wuxia_getMap.jar feature map
4. hadoop jar Wuxia_pagerank.jar map result
5. hadoop jar Wuxia_LPA.jar map LPA
