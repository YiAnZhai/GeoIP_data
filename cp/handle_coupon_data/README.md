# Data Handle - Twitter && API 数据处理
## Twitter 处理流程
### 入口文件 handle_twitter/coupon_auto_run.sh
#### 1、跑取 twitter 中的短网址
```
cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
scrapy crawl getredirecturl -a api="twitter" -L WARNING
```
`twitter_coupon_info_handle 表` 中的 twitter 原始数据中的 `expand_url` 跑取最终的 url，存入 `get_red_url 表`

#### 2、get_red_url 表 数据判断
```
cd /home/moma/Documents/handle_coupon_data/handle_twitter
python update_brandingetredurl.py
```
`redurl.handle_redurl()`
2.1、`get_red_url 表` `twitter_coupon_info_handle表` 联查
2.2、`get_red_url 表` 对 url 进行 extract_domain
2.3、`get_red_url 表` 对 url 进行 judge_black

#### ～、twitter 4个单独处理的 domain by yifan
```
python handler_linkis.py
```
从 url 中提取 website；website 同时也为此 coupon 的 landing_url(redirect_url)
```
python handler_constantcontact.py
python handler_campagin.py
```
从最终页面中提取 website（出现最多的）；同时提取 landing_url(出现最多 website 第一个)

最终直接插入 `coupon_twitter_today_copy 表`

[相关文档](https://docs.google.com/document/d/1OWW8sndyTQWeIfKLmGcnd0XbdfISFRmShX9NkhfmP58/edit)
> this file create by yifan
