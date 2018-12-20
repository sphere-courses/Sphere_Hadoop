Page Rank TOP:

1.	8737
2.	2914
3.	18964
4.	1220
5.	2409
6.	10029
7.	214538
8.	7343
9.	39295
10.	38283
11.	18963

При работе на Spark c архивами необходимо указывать параметр отвечающий за переразбиение RDD после его создания из архива,
т.к. в ином случае spark задача будет выполняться без параллелизма на одной машине. 
При правильной установке этого параметра получается добиться существенного преимущества в скорости по сравнению с Hadoop: 
20 итераций на spark требуют порядка 25 минут, в то время как Hadoop требует как минимум 1 час.