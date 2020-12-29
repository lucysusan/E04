#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('random_forest').getOrCreate()
df=spark.read.csv('hdfs:///e04/data/df_train_noindex.csv',inferSchema=True,header=True)


# In[2]:


df.dtypes


# In[3]:


from pyspark.ml.feature import VectorAssembler
df_assembler = VectorAssembler(inputCols=['age_range','gender','total_logs','unique_item_ids','categories','browse_days','one_clicks','shopping_carts','purchase_times','favourite_times'], outputCol="features")
df = df_assembler.transform(df)
df.printSchema()


# In[4]:


df.select(['features','label']).show(10,False)


# In[5]:


model_df = df.select(['features','label'])
train_df,test_df=model_df.randomSplit([0.7,0.3]) 


# In[6]:


from pyspark.ml.classification import RandomForestClassifier
rf_classifier=RandomForestClassifier(labelCol='label',numTrees=200).fit(train_df)
rf_predictions=rf_classifier.transform(test_df)


# In[7]:


print('{}{}'.format('评估每个属性的重要性:',rf_classifier.featureImportances))


# In[8]:


rf_predictions.select(['probability','label','prediction']).show(10,False)


# In[9]:


from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

rf_accuracy=MulticlassClassificationEvaluator(labelCol='label',metricName='accuracy').evaluate(rf_predictions)
print('MulticlassClassificationEvaluator 随机深林测试的准确性： {0:.0%}'.format(rf_accuracy))


# In[10]:


rf_auc=BinaryClassificationEvaluator(labelCol='label').evaluate(rf_predictions)
print('BinaryClassificationEvaluator 随机深林测试的准确性： {0:.0%}'.format(rf_auc))

