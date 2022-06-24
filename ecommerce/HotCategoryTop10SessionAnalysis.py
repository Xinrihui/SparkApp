#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os

from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def topK_category_list(facts_table, K=10):
    def f_flatmap(line):
        """

        :param line:  文本的一行
        :return:
        """
        arr = line.split('_')
        category = arr[6]
        order = arr[8]
        pay = arr[10]

        if category != '-1':
            return [(category, (1, 0, 0))]

        elif order != 'null':

            ids = order.split(',')

            tuples = []

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    tuples.append((id, (0, 1, 0)))

            return tuples

        elif pay != 'null':

            ids = pay.split(',')

            tuples = []

            if len(ids) > 0:

                for id in ids:
                    # if id != 'null':
                    tuples.append((id, (0, 0, 1)))

            return tuples

        else:
            return []

    category_order_pay_rdd = facts_table.flatMap(f_flatmap)

    def f_reducebykey(value1, value2):

        category = value1[0] + value2[0]
        order = value1[1] + value2[1]
        pay = value1[2] + value2[2]

        return (category, order, pay)

    count_category_order_pay_rdd = category_order_pay_rdd.reduceByKey(f_reducebykey)

    # 2. 将品类进行排序，并且取前10名
    # 点击数量排序，下单数量排序，支付数量排序
    # 元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
    # ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )

    result = count_category_order_pay_rdd.sortBy(lambda x: x[1], ascending=False)
    # [('15', (6120, 1672, 1259)), ('2', (6119, 1767, 1196)), ('20', (6098, 1776, 1244)), ('12', (6095, 1740, 1218)), ('11', (6093, 1781, 1202)), ('17', (6079, 1752, 1231)), ('7', (6074, 1796, 1252)), ('9', (6045, 1736, 1230)), ('19', (6044, 1722, 1158)), ('13', (6036, 1781, 1161))]

    return result.take(K)


def run(file_dir):
    conf = SparkConf()
    conf.setMaster("local").setAppName("HotCategoryTop10Analysis")

    # 配置超时时间
    conf.set("spark.executor.heartbeatInterval", 120)  # 120s

    sc = SparkContext(conf=conf)

    sliceNum = 8

    facts_table = sc.textFile(file_dir, sliceNum)

    facts_table.cache()

    top10_category = [ele[0] for ele in topK_category_list(facts_table, 10)]

    print(top10_category)

    top10_category = set(top10_category)

    def f_filter(line):
        """

        :param line:  文本的一行
        :return:
        """
        arr = line.split('_')
        category = arr[6]

        if category != '-1':

            if category in top10_category:
                return True

        return False

    rdd_filter = facts_table.filter(f_filter)

    def f_map(line):
        """

        :param line:  文本的一行
        :return:
        """
        arr = line.split('_')

        # arr[6] category
        # arr[2] sessionid
        return (arr[6], arr[2]), 1

    rdd_session_count = rdd_filter.map(f_map).reduceByKey(lambda x, y: x + y)

    # print(rdd_session_count.take(10))

    rdd_groupby = rdd_session_count.groupBy(lambda x: x[0][0])

    print(rdd_groupby.take(10))

    # [('12', < pyspark.resultiterable.ResultIterable object at 0x000002B8569DCEC8 >),
    #  ('20', < pyspark.resultiterable.ResultIterable object at 0x000002B8569DCE88 >)

    def f_mapValues(results):

        res = []

        for ele in sorted(list(results), key=lambda x: x[1], reverse=True)[:10]:
            res.append((ele[0][1], ele[1]))

        return res

    rdd_session_top10 = rdd_groupby.mapValues(f_mapValues)

    print(rdd_session_top10.take(10))

    # [('12', [('22a687a0-07c9-4e84-adff-49dfc4fe96df', 8), ('a4b05ea2-2869-4f20-a82a-86352aa60e9f', 8),
    #          ('b4589b16-fb45-4241-a576-28f77c6e4b96', 8), ('73203aee-de2e-443e-93cb-014e38c0d30c', 8),
    #          ('a735881e-4c30-4ddc-a1d9-ef2069d5fb5b', 7), ('ab16e1e4-b3fc-4d43-9c95-3d49ec26d59c', 7),
    #          ('4c90a8a8-91d0-4888-908c-95dad1c5194e', 7), ('89278c1a-1e33-45aa-a4b7-33223e37e9df', 7),
    #          ('c32dc073-4454-4fcd-bf55-fbfcc8e650f3', 7), ('64285623-54ad-4a1f-ae84-d8f85ebf94c6', 7)]),
    # ('20', [
    #     ('199f8e1d-db1a-4174-b0c2-ef095aaef3ee', 8), ('7eacf77a-c019-4072-8e09-840e5cca6569', 8),
    #     ('07b5fb82-da25-4968-9fd8-47485f4cf61e', 7), ('d500c602-55db-4eb7-a343-3540c3ec7a36', 7),
    #     ('22e78a14-c5eb-45fe-a67d-2ce538814d98', 7), ('cde33446-095b-433c-927b-263ba7cd102a', 7),
    #     ('215bdee7-db27-458d-80f4-9088d2361a2e', 7), ('85157915-aa25-4a8d-8ca0-9da1ee67fa70', 7),
    #     ('5e3545a0-1521-4ad6-91fe-e792c20c46da', 7), ('ab27e376-3405-46e2-82cb-e247bf2a16fb', 7)]), ('7', [
    #     ('a41bc6ea-b3e3-47ce-98af-48169da7c91b', 9), ('9fa653ec-5a22-4938-83c5-21521d083cd0', 7),
    #     ('aef6615d-4c71-4d39-8062-9d5d778e55f1', 7), ('f34878b8-1784-4d81-a4d1-0c93ce53e942', 7),
    #     ('2d4b9c3e-2a9e-41b6-9573-9fde3533ed89', 7), ('4dbd319c-3f44-48c9-9a71-a917f1d922c1', 7),
    #     ('95cb71b8-7033-448f-a4db-ae9861dd996b', 7), ('fde62452-7c09-4733-9655-5bd3fb705813', 6),
    #     ('5f513108-339d-4ec4-97dd-f5422bebc974', 6), ('c7ebecf5-5236-4daa-8165-2ac1511d44bd', 6)]), ('9', [
    #     ('199f8e1d-db1a-4174-b0c2-ef095aaef3ee', 9), ('329b966c-d61b-46ad-949a-7e37142d384a', 8),
    #     ('5e3545a0-1521-4ad6-91fe-e792c20c46da', 8), ('66c96daa-0525-4e1b-ba55-d38a4b462b97', 7),
    #     ('e306c00b-a6c5-44c2-9c77-15e919340324', 7), ('f205fd4f-f312-46d2-a850-26a16ac2734c', 7),
    #     ('8a0f8fe1-d0f4-4687-aff3-7ce37c52ab71', 7), ('cbdbd1a4-7760-4195-bfba-fa44492bf906', 7),
    #     ('8f9723a3-833d-4103-a7ff-352cd17de067', 7), ('bed60a57-3f81-4616-9e8b-067445695a77', 7)]), ('17', [
    #     ('4509c42c-3aa3-4d28-84c6-5ed27bbf2444', 12), ('bf390289-5c9d-4037-88b3-fdf386b3acd5', 8),
    #     ('dd3704d5-a2f9-40c1-b491-87d24bbddbdb', 8), ('0416a1f7-350f-4ea9-9603-a05f8cfa0838', 8),
    #     ('9bdc044f-8593-49fc-bbf0-14c28f901d42', 8), ('1b5ac69b-5e00-4ff3-8a5c-6822e92ecc0c', 8),
    #     ('08e65f4b-b2b0-4efd-8b66-92d2222465b9', 7), ('ab16e1e4-b3fc-4d43-9c95-3d49ec26d59c', 7),
    #     ('abbf9c96-eca3-4ecf-9c44-04193eb4b562', 7), ('fde62452-7c09-4733-9655-5bd3fb705813', 7)]), ('11', [
    #     ('329b966c-d61b-46ad-949a-7e37142d384a', 12), ('4509c42c-3aa3-4d28-84c6-5ed27bbf2444', 7),
    #     ('99f48b83-8f85-4bea-8506-c78cfe5a2136', 7), ('dc226249-ce13-442c-b6e4-bfc84649fff6', 7),
    #     ('2cd89b09-bae3-49b5-a422-9f9e0c12a040', 7), ('73203aee-de2e-443e-93cb-014e38c0d30c', 6),
    #     ('c9f1b658-ac62-4263-b118-f95221c1189b', 6), ('5d2f3efb-be1c-4ee2-8fd5-545fd049e70c', 6),
    #     ('66c96daa-0525-4e1b-ba55-d38a4b462b97', 6), ('82bb0a33-5f2b-4a94-ac5b-c1273f7cbddc', 6)]), ('15', [
    #     ('632972a4-f811-4000-b920-dc12ea803a41', 10), ('66a421b0-839d-49ae-a386-5fa3ed75226f', 8),
    #     ('9fa653ec-5a22-4938-83c5-21521d083cd0', 8), ('5e3545a0-1521-4ad6-91fe-e792c20c46da', 8),
    #     ('f34878b8-1784-4d81-a4d1-0c93ce53e942', 8), ('b39b8d5a-d503-4fff-9b53-13f334716d9f', 7),
    #     ('394490ea-4e99-4b83-a79a-a97aaee5b021', 7), ('84d87899-dacf-48e4-8cc2-fab368b6984c', 7),
    #     ('89278c1a-1e33-45aa-a4b7-33223e37e9df', 7), ('6bc240fb-ba5a-4661-b757-ab7d193055b3', 7)]), ('19', [
    #     ('85157915-aa25-4a8d-8ca0-9da1ee67fa70', 9), ('d4c2b45d-7fa1-4eff-8473-42cecdaffd62', 9),
    #     ('fde62452-7c09-4733-9655-5bd3fb705813', 9), ('329b966c-d61b-46ad-949a-7e37142d384a', 8),
    #     ('1b5e5ce7-cd04-4e78-9a6f-1c3dbb29ce39', 8), ('b61734fd-b10d-456d-b189-2e3fe0adf31d', 7),
    #     ('5c73cc38-f7c8-4d99-870e-e61bec9a91de', 7), ('5e3545a0-1521-4ad6-91fe-e792c20c46da', 7),
    #     ('46d3e83c-11c1-4aa0-a2be-8999ead49879', 7), ('4d93913f-a892-490d-aa58-3a74b9099e29', 7)]), ('13', [
    #     ('329b966c-d61b-46ad-949a-7e37142d384a', 8), ('f736ee4a-cc14-4aa9-9a96-a98b0ad7cc3d', 8),
    #     ('7eacce38-ffbc-4f9c-a3ee-b1711f8927b0', 7), ('1b5e5ce7-cd04-4e78-9a6f-1c3dbb29ce39', 7),
    #     ('0f227059-7006-419c-87b0-b2057b94505b', 7), ('c0f70b31-fc3b-4908-af97-9b4936340367', 7),
    #     ('632972a4-f811-4000-b920-dc12ea803a41', 7), ('1fb79ba2-4780-4652-9574-b1577c7112db', 7),
    #     ('a1cef5b4-9451-480f-9c9a-a52c9404a51a', 6), ('3d1e9586-922d-446d-b881-13a2b6c6f06b', 6)]), ('2', [
    #     ('b4589b16-fb45-4241-a576-28f77c6e4b96', 11), ('66c96daa-0525-4e1b-ba55-d38a4b462b97', 11),
    #     ('f34878b8-1784-4d81-a4d1-0c93ce53e942', 10), ('25fdfa71-c85d-4c28-a889-6df726f08ffb', 9),
    #     ('213bc2d5-be6b-49a3-9cb6-f9afc5b69b3d', 8), ('ab27e376-3405-46e2-82cb-e247bf2a16fb', 8),
    #     ('0b17692b-d603-479e-a031-c5001ab9009e', 8), ('f666d6ba-b3e8-45b1-a269-c5d6c08413c3', 8),
    #     ('39cd210e-9d54-4315-80bf-bed004996861', 8), ('aada57d4-9553-4abf-8152-ca1e72be04e7', 7)])]


if __name__ == '__main__':
    data_dir = '../data/'

    file_dir = os.path.join(data_dir, 'user_visit_action.txt')

    run(file_dir)
