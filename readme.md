## 运行
### 环境搭建
通过conda或者venv构建Python 3.13.5的环境，安装requirements.txt的环境
venv, 需要提前自行安装Python 3.13.5
```
    python -m venv myenv
    cd myenv
    myenv\Scripts\activate.bat
    pip install -r requirements.txt
```
conda
```
    conda create -n myenv python=3.13.5
    conda activate myenv
    pip install -r requirements.txt
```
### 运行程序
`python -m flask --app app_local run --port 8000`

## 设计架构

- 前端：Flask，Jinja2和Javascript。 使用HTML构建网页的大框架，使用Jinja把功能层的参数和方法传到前端，使用Javascript构建可互动的模块。
- 后端：Python以及数据处理相关的模块。后端使用Pandas处理数据表，使用sqlalchemy实现和数据库交互，pyodbc连接SQL Server
- 数据库：本地版本使用Sqlite进行测试，服务器版本使用Sql Server Database

## 运行逻辑

1. 根据数据库中已有数据构建前端
2. 用户根据界面操作调用端口进行数据操作
3. 根据变化更新数据库，更新前端
4. 循环往复

## 功能介绍

### 基础数据管理 Basic Data Maintenance
1. Delete Data: 清空数据库
2. PO Name：上传和修改PO名称
3. BU Name：上传和修改BU名称，并且改变其PO的丛书关系
4. Project Category：上传修改项目类型
5. Staff Categories：上传修改用工类型 
6. Modify users：为修改用户信息部分预留了UI但是功能没有实装

### 预测管理
1. Create Project：上传，删除和修改项目的名称，类型，从属的PO和BU和IO号。IO号可以一个项目添加多个。项目支持级联删除，会同时删除相关的预测和Capex预测
2. Create Forecast：上传，删除和修改预测的各项数值。上传时需要指定PO，BU，年份和财年。预测的输入分为两部分，人员和非人员预测。非人员预测靠直接输入，人员预测靠输入用工时长同时结合已经输入的用工费率进行计算。修改的
3. Capex forecast上传，删除和修改Capex预测。上传时需要根据下拉菜单选择已经输入过的PO，BU，财年和项目，然后手动输入描述，预测和cost center。修改的时候可以修改所有的数值。

### Funding管理
1. Create Funding：上传删除和修改提供资金的信息。上传时通过下拉菜单根据从属选取已经上传的PO和BU，手动输入其他信息

### 预算管理
1. Create Budget：上传删除和修改预算。
1. Capex Budget：上传删除和修改Capex预算。两者均采用和上传Funding同样的方法

### 数据可视化
1. Display Raw Data：以表格形式显示数据库中储存的原始数据
2. Data Summary：统计每个部门资金和Capex的预测，预算和实际支出
3. Project Summary：统计每个项目的资金和Capex预测和实际支出

### 财务相关的数据维护
1. Modify Staff Cost：根据部门和年份上传不同用工类型每年的费率
2. Upload actual expenditures：上传

## 代码模块
- /app_local 使用本地sqlite数据库的版本，是目前版本的主体部分，以上提到的所有数据处理的功能都集成在这个模块中。
- /app_sql 使用sql server database存储数据的版本，完成了连接本地部署的sql server database的部分以及部分上传的sql query
- /backend 后端处理数据和上传数据库的算法
- /sql 包含软件使用中需要用到的sql query，包括创建表格，更新表格，获取数据和删除表格。
- /app_local/static/js 前端的可交互元素，包括了每个页面主体上的可交互数据表格和添加修改数据时弹出的表格。
- /app_local/templates 前端的静态页面。basic_page是除login以外的页面的基础模版。sidebar定义页边导航栏为一个独立的元素。
- /app_local/templates/pages 中的每个页面对应导航栏中的每一个功能模块
- /app_local 中其余的python文件构建了不同功能模块的路由器，每个文件的名称都和其功能模块对应。不过modify_tables.py为除了projects和forecast的部分提供了一个基础模版，包含了添加，修改和删除的endpoint。细化的文件中对各自独有的数据表的独特列做了细化。

## 尚未开发的功能
1. 用户认证
2. 根据不同用户组区分功能
3. 部署云端数据库
4. 支持多用户同时访问
5. 用户界面的美化
6. 通过表格上传历史数据

## bug和存在的问题
1. 上传支出时不支持中文和特殊字符（需要修改pandas读取时的text encoding）
2. 数据表和资金相关的表头缺少单位
3. 需要优化上传数据库的代码，区分大规模上传和小规模上传以加快运行速度
4. 需要进行前后端代码分离，并且在后端代码中分离服务层和数据库层的代码。现在虽然数据库代码主要集中在/backend中但是一部分数据处理还是混在了/app_local的页面渲染/前后端数据沟通的代码里。



