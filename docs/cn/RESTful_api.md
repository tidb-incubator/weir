# API接口

## 移除 namespace

#### Request
- Method: **POST**
- URL:  ```/admin/namespace/remove/:namespace```
- Headers：

#### Response
- Body
```
{
    "code":200,
    "msg":"success"
}
```

#### 错误码

| 错误码 | 信息 |
| --- | --- |
| 400 | bad namespace parameter |
| 200 | success |


## 准备重新加载 namespace

#### Request
- Method: **POST**
- URL:  ```/admin/namespace/reload/prepare/:namespace```

#### Response
- Body
```
{
    "code":200,
    "msg":"success"
}
```

#### 错误码

| 错误码 | 信息 |
| --- | --- |
| 400 | bad namespace parameter |
| 500 | get namespace value from configcenter error |
| 500 | prepare reload namespace error |
| 200 | success |


## 提交重新加载 namespace

#### Request
- Method: **POST**
- URL:  ```/admin/namespace/reload/commit/:namespace```

#### Response
- Body
```
{
    "code":200,
    "msg":"success"
}
```

#### 错误码

| 错误码 | 信息 |
| --- | --- |
| 400 | bad namespace parameter |
| 500 | commit reload namespace error |
| 200 | success |