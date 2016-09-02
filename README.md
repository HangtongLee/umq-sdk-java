# UMQ Java SDK 使用说明

## 相关配置
将resources/ucloud_umq.properties文件拷贝到HOME目录下，并在其中加上你自己的配置。

## HTTP 调用

### 队列相关
相关示例请查看src/test/java/com/ucloud/umq/action/QueueActionTest.java

| 功能  | 函数|备注|
|:-----|:-----|:----|
|创建|QueueAction.createQueue||
|删除|QueueAction.deleteQueue||
|获取|QueueAction.listQueue||

### 角色相关
相关示例请查看src/test/java/com/ucloud/umq/action/RoleActionTest.java

| 功能  | 函数|备注|
|:-----|:-----|:----|
|创建|RoleAction.createRole||
|删除|RoleAction.deleteRole||
|获取|QueueAction.listQueue|会返回queue下的相关角色信息|

### 消息相关
相关示例请查看src/test/java/com/ucloud/umq/action/MessageActionTest.java

| 功能  | 函数|备注|
|:-----|:-----|:----|
|发送|MessageAction.publishMsg||
|接收|MessageAction.pullMsg||
|确认收到|MessageAction.ackMsg||

## WebSocket 消费信息
使用WebSocket协议消费消息请查看demo，代码位于：src/main/java/com/ucloud/umq/client/WebsocketClientDemo.java
