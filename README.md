## 项目介绍
```

1 自动生成同步任务以及数仓任务执行脚本。
2 通过约束（表名和python任务名一样）自动生成任务血缘依赖。
3 同步任务和数仓任务一键部署至Azkaban。
4 通过git hook 实现协同开发。

```

## 技术架构
```
1 git           协同开发
2 azkaban       任务调度
3 sqoop         任务同步
4 haddop+spark  数据存储+计算
5 mysql         组件元数据存储
```

## 数据仓库层级

```
apps.
    ├── __init__.py
    ├── dm   // 数据集市（应用层）
    ├── dwt  // 主题层
    ├── dws  // 数据汇总层
    ├── dwd  // 数据明细层
    ├── ods  // 数据贴源  （可直接供下游dwd使用）
    └── src  // 系统贴源层 （系统溯源数据,无任何人为操作）
```

## 数据同步任务配置
config.

## 工程任务自动生成

```
generators.
├── config.py          //解析数据配置
├── generate_blood.py  //调度工程自动生成脚本，并且发布至azkaban
├── template_file      //脚本模版
```

