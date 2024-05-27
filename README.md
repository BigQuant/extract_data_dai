# 数据抽取

## 模块介绍

- 抽取 DAI 数据
- [文档](https://bigquant.com/wiki/doc/aistudio-HVwrgP4J1A#h-数据抽取dai5)

## 模块开发

[BigQuant AIStudio 可视化模块开发文档](https://bigquant.com/wiki/doc/aistudio-okn4EnwWe1)

进入 [BigQuant AIStudio](https://bigquant.com/aistudio) 命令行终端开发模块

### clone module git project and cd

```bash
git clone "项目地址"
cd "本地 git 项目目录"
```

### 安装模块到开发路径，在开发里默认安装为 v0 版本

bq module install --dev

### 测试模块

在可视化开发模块列表找到对应模块，或者通过代码访问(x, y替换为具体的模块名和版本号):

```bash
python3 -c "from bigmodule import M; M.x.y()"
```

以当前模块为例，示例如下

```bash
python3 -c "from bigmodule import M; M.extract_data_dai.v0()"
```

### 测试完成后卸载开发环境模块

```bash
bq module uninstall --dev
```

### 发布模块到模块库，以用于正式使用

```bash
bq module publish
```
