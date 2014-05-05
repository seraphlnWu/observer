# coding=utf8
from observer.utils import json_loads
from observer import log

class SinaServerError(Exception):
    ''' '''

    def __init__(self, code, msg, method):
        ''' '''
        msg = str(code) + ':' + msg
        Exception.__init__(self, msg)
        self.code = code
        self.method = method


class SinaServerLimitReached(SinaServerError): pass


class SinaServerErrorNoRetry(SinaServerError): pass


class SinaServerRecordLimitReached(SinaServerError): pass


ERRORS = { 10001: (SinaServerError, "System error "), 	#系统错误
           10002: (SinaServerError, "Service unavailable "),	# 服务暂停
           10003: (SinaServerError, "Remote service error "),	# 远程服务错误
           10004: (SinaServerError, "IP limit "),	# IP限制不能请求该资源
           10005: (SinaServerErrorNoRetry, "Permission denied, need a high level appkey "),	# 该资源需要appkey拥有授权
           10006: (SinaServerErrorNoRetry, "Source paramter (appkey) is missing "),	# 缺少source (appkey) 参数
           10007: (SinaServerErrorNoRetry, "Unsupport mediatype (%s) "),	# 不支持的MediaType (%s)
           10008: (SinaServerErrorNoRetry, "Param error, see doc for more info "),	# 参数错误，请参考API文档
           10009: (SinaServerError, "Too many pending tasks, system is busy "),	# 任务过多，系统繁忙
           10010: (SinaServerError, "Job expired "),	# 任务超时
           10011: (SinaServerError, "RPC error "),	# RPC错误
           10012: (SinaServerError, "Illegal request "),	# 非法请求
           10013: (SinaServerError, "Invalid weibo user "),	# 不合法的微博用户
           10014: (SinaServerErrorNoRetry, "Insufficient app permissions "),	# 应用的接口访问权限受限
           10016: (SinaServerErrorNoRetry, "Miss required parameter (%s) , see doc for more info "),	# 缺失必选参数 (%s)，请参考API文档
           10017: (SinaServerErrorNoRetry, "Parameter (%s)'s value invalid, expect (%s) , but get (%s) , see doc for more info "),	# 参数值非法，需为 (%s)，实际为 (%s)，请参考API文档
           10018: (SinaServerError, "Request body length over limit "),	# 请求长度超过限制
           10020: (SinaServerErrorNoRetry, "Request api not found "),	# 接口不存在
           10021: (SinaServerErrorNoRetry, "HTTP method is not suported for this request "),	# 请求的HTTP METHOD不支持，请检查是否选择了正确的POST/GET方式
           10022: (SinaServerLimitReached, "IP requests out of rate limit "),	# IP请求频次超过上限
           10023: (SinaServerLimitReached, "User requests out of rate limit "),	# 用户请求频次超过上限
           10024: (SinaServerLimitReached, "User requests for (%s) out of rate limit "),	# 用户请求特殊接口 (%s) 频次超过上限
           20001: (SinaServerErrorNoRetry, "IDs is null "),	# IDs参数为空
           20002: (SinaServerErrorNoRetry, "Uid parameter is null "),	# Uid参数为空
           20003: (SinaServerErrorNoRetry, "User does not exists "),	# 用户不存在
           20005: (SinaServerErrorNoRetry, "Unsupported image type, only suport JPG, GIF, PNG "),	# 不支持的图片类型，仅仅支持JPG、GIF、PNG
           20006: (SinaServerErrorNoRetry, "Image size too large "),	# 图片太大
           20007: (SinaServerErrorNoRetry, "Does multipart has image "),	# 请确保使用multpart上传图片
           20008: (SinaServerErrorNoRetry, "Content is null "),	# 内容为空
           20009: (SinaServerErrorNoRetry, "IDs is too many "),	# IDs参数太长了
           20012: (SinaServerErrorNoRetry, "Text too long, please input text less than 140 characters "),	# 输入文字太长，请确认不超过140个字符
           20013: (SinaServerErrorNoRetry, "Text too long, please input text less than 300 characters "),	# 输入文字太长，请确认不超过300个字符
           20014: (SinaServerError, "Param is error, please try again "),	# 安全检查参数有误，请再调用一次
           20015: (SinaServerError, "Account or ip or app is illgal, can not continue "),	# 账号、IP或应用非法，暂时无法完成此操作
           20016: (SinaServerLimitReached, "Out of limit "),	# 发布内容过于频繁
           20017: (SinaServerErrorNoRetry, "Repeat content "),	# 提交相似的信息
           20018: (SinaServerErrorNoRetry, "Contain illegal website "),	# 包含非法网址
           20019: (SinaServerErrorNoRetry, "Repeat conetnt "),	# 提交相同的信息
           20020: (SinaServerErrorNoRetry, "Contain advertising "),	# 包含广告信息
           20021: (SinaServerErrorNoRetry, "Content is illegal "),	# 包含非法内容
           20022: (SinaServerError, "Your ip's behave in a comic boisterous or unruly manner "),	# 此IP地址上的行为异常
           20031: (SinaServerError, "Test and verify "),	# 需要验证码
           20032: (SinaServerErrorNoRetry, "Update success, while server slow now, please wait 1-2 minutes "),	# 发布成功，目前服务器可能会有延迟，请耐心等待1-2分钟

           20101: (SinaServerErrorNoRetry, "Target weibo does not exist "),	# 不存在的微博
           20102: (SinaServerErrorNoRetry, "Not your own weibo "),	# 不是你发布的微博
           20103: (SinaServerErrorNoRetry, "Can't repost yourself weibo "),	# 不能转发自己的微博
           20104: (SinaServerErrorNoRetry, "Illegal weibo "),	# 不合法的微博
           20109: (SinaServerErrorNoRetry, "Weibo id is null "),	# 微博ID为空
           20111: (SinaServerErrorNoRetry, "Repeated weibo text "),	# 不能发布相同的微博

           20201: (SinaServerErrorNoRetry, "Target weibo comment does not exist "),	# 不存在的微博评论
           20202: (SinaServerErrorNoRetry, "Illegal comment "),	# 不合法的评论
           20203: (SinaServerErrorNoRetry, "Not your own comment "),	# 不是你发布的评论
           20204: (SinaServerErrorNoRetry, "Comment id is null "),	# 评论ID为空

           20301: (SinaServerErrorNoRetry, "Can't send direct message to user who is not your follower "),	# 不能给不是你粉丝的人发私信
           20302: (SinaServerErrorNoRetry, "Illegal direct message "),	# 不合法的私信
           20303: (SinaServerErrorNoRetry, "Not your own direct message "),	# 不是属于你的私信
           20305: (SinaServerErrorNoRetry, "Direct message does not exist "),	# 不存在的私信
           20306: (SinaServerErrorNoRetry, "Repeated direct message text "),	# 不能发布相同的私信
           20307: (SinaServerErrorNoRetry, "Illegal direct message id "),	# 非法的私信ID

           20401: (SinaServerError, "Domain not exist "),	# 域名不存在
           20402: (SinaServerError, "Wrong verifier "),	# Verifier错误

           20501: (SinaServerErrorNoRetry, "Source_user or target_user does not exists "),	# 参数source_user或者target_user的用户不存在
           20502: (SinaServerErrorNoRetry, "Please input right target user id or screen_name "),	# 必须输入目标用户id或者screen_name
           20503: (SinaServerErrorNoRetry, "Need you follow user_id "),	# 参数user_id必须是你关注的用户
           20504: (SinaServerErrorNoRetry, "Can not follow yourself "),	# 你不能关注自己
           20505: (SinaServerError, "Social graph updates out of rate limit "),	# 加关注请求超过上限
           20506: (SinaServerErrorNoRetry, "Already followed "),	# 已经关注此用户
           20507: (SinaServerError, "Verification code is needed "),	# 需要输入验证码
           20508: (SinaServerErrorNoRetry, "According to user privacy settings,you can not do this "),	# 根据对方的设置，你不能进行此操作
           20509: (SinaServerErrorNoRetry, "Private friend count is out of limit "),	# 悄悄关注个数到达上限
           20510: (SinaServerErrorNoRetry, "Not private friend "),	# 不是悄悄关注人
           20511: (SinaServerErrorNoRetry, "Already followed privately "),	# 已经悄悄关注此用户
           20512: (SinaServerErrorNoRetry, "Please delete the user from you blacklist before you follow the user "),	# 你已经把此用户加入黑名单，加关注前请先解除
           20513: (SinaServerErrorNoRetry, "Friend count is out of limit! "),	# 你的关注人数已达上限
           20521: (SinaServerErrorNoRetry, "Hi Superman, you have concerned a lot of people, have a think of how to make other people concern about you! ! If you have any questions, please contact Sina customer service: 400 690 0000 "),	# hi 超人，你今天已经关注很多喽，接下来的时间想想如何让大家都来关注你吧！如有问题，请联系新浪客服：400 690 0000
           20522: (SinaServerErrorNoRetry, "Not followed "),	# 还未关注此用户
           20523: (SinaServerErrorNoRetry, "Not followers "),	# 还不是粉丝
           20524: (SinaServerErrorNoRetry, "Hi Superman, you have cancelled concerning a lot of people, have a think of how to make other people concern about you! ! If you have any questions, please contact Sina customer service: 400 690 0000 "),	# hi 超人，你今天已经取消关注很多喽，接下来的时间想想如何让大家都来关注你吧！如有问题，请联系新浪客服：400 690 0000

           20601: (SinaServerErrorNoRetry, "List name too long, please input text less than 10 characters "),	# 列表名太长，请确保输入的文本不超过10个字符
           20602: (SinaServerErrorNoRetry, "List description too long, please input text less than 70 characters "),	# 列表描叙太长，请确保输入的文本不超过70个字符
           20603: (SinaServerErrorNoRetry, "List does not exists "),	# 列表不存在
           20604: (SinaServerErrorNoRetry, "Only the owner has the authority "),	# 不是列表的所属者
           20605: (SinaServerErrorNoRetry, "Illegal list name or list description "),	# 列表名或描叙不合法
           20606: (SinaServerErrorNoRetry, "Object already exists "),	# 记录已存在
           20607: (SinaServerError, "DB error, please contact the administator "),	# 数据库错误，请联系系统管理员
           20608: (SinaServerErrorNoRetry, "List name duplicate "),	# 列表名冲突
           20610: (SinaServerErrorNoRetry, "Does not support private list "),	# 目前不支持私有分组
           20611: (SinaServerError, "Create list error "),	# 创建列表失败
           20612: (SinaServerErrorNoRetry, "Only support private list "),	# 目前只支持私有分组
           20613: (SinaServerErrorNoRetry, "You hava subscriber too many lists "),	# 订阅列表达到上限
           20614: (SinaServerErrorNoRetry, "Too many lists, see doc for more info "),	# 创建列表达到上限，请参考API文档
           20615: (SinaServerErrorNoRetry, "Too many members, see doc for more info "),	# 列表成员上限，请参考API文档

           20701: (SinaServerErrorNoRetry, "Repeated tag text "),	# 不能提交相同的收藏标签
           20702: (SinaServerErrorNoRetry, "Tags is too many "),	# 最多两个收藏标签
           20703: (SinaServerErrorNoRetry, "Illegal tag name "),	# 收藏标签名不合法

           20801: (SinaServerErrorNoRetry, "Trend_name is null "),	# 参数trend_name是空值
           20802: (SinaServerErrorNoRetry, "Trend_id is null "),	# 参数trend_id是空值

           20901: (SinaServerErrorNoRetry, "Error: in blacklist "),	# 错误:已经添加了黑名单
           20902: (SinaServerErrorNoRetry, "Error: Blacklist limit has been reached. "),	# 错误:已达到黑名单上限
           20903: (SinaServerErrorNoRetry, "Error: System administrators can not be added to the blacklist. "),	# 错误:不能添加系统管理员为黑名单
           20904: (SinaServerErrorNoRetry, "Error: Can not add yourself to the blacklist. "),	# 错误:不能添加自己为黑名单
           20905: (SinaServerErrorNoRetry, "Error: not in blacklist "),	# 错误:不在黑名单中

           21001: (SinaServerErrorNoRetry, "Tags parameter is null "),	# 标签参数为空
           21002: (SinaServerErrorNoRetry, "Tags name too long "),	# 标签名太长，请确保每个标签名不超过14个字符

           21101: (SinaServerErrorNoRetry, "Domain parameter is error "),	# 参数domain错误
           21102: (SinaServerErrorNoRetry, "The phone number has been used "),	# 该手机号已经被使用
           21103: (SinaServerErrorNoRetry, "The account has bean bind phone "),	# 该用户已经绑定手机
           21104: (SinaServerError, "Wrong verifier "),	# Verifier错误

           21301: (SinaServerError, "Auth faild "),	# 认证失败
           21302: (SinaServerErrorNoRetry, "Username or password error "),	# 用户名或密码不正确
           21303: (SinaServerError, "Username and pwd auth out of rate limit "),	# 用户名密码认证超过请求限制
           21304: (SinaServerErrorNoRetry, "Version rejected "),	# 版本号错误
           21305: (SinaServerErrorNoRetry, "Parameter absent "),	# 缺少必要的参数
           21306: (SinaServerError, "Parameter rejected "),	# OAuth参数被拒绝
           21307: (SinaServerError, "Timestamp refused "),	# 时间戳不正确
           21308: (SinaServerError, "Nonce used "),	# 参数nonce已经被使用
           21309: (SinaServerErrorNoRetry, "Signature method rejected "),	# 签名算法不支持
           21310: (SinaServerError, "Signature invalid "),	# 签名值不合法
           21311: (SinaServerError, "Consumer key unknown "),	# 参数consumer_key不存在
           21312: (SinaServerError, "Consumer key refused "),	# 参数consumer_key不合法
           21313: (SinaServerErrorNoRetry, "Miss consumer key "),	# 参数consumer_key缺失
           21314: (SinaServerError, "Token used "),	# Token已经被使用
           21315: (SinaServerError, "Token expired "),	# Token已经过期
           21316: (SinaServerError, "Token revoked "),	# Token不合法
           21317: (SinaServerError, "Token rejected "),	# Token不合法
           21318: (SinaServerError, "Verifier fail "),	# Pin码认证失败
           21319: (SinaServerError, "Accessor was revoked "),	# 授权关系已经被解除
           21320: (SinaServerErrorNoRetry, "OAuth2 must use https "),	# 使用OAuth2必须使用https
           21321: (SinaServerLimitReached, "Applications over the unaudited use restrictions "),	# 未审核的应用使用人数超过限制
           21327: (SinaServerError, "Expired token "),	# token过期
           21411: (SinaServerRecordLimitReached, "Reached the result limit "), #超过数量限制

           21501: (SinaServerErrorNoRetry, "Urls is null "),	# 参数urls是空的
           21502: (SinaServerErrorNoRetry, "Urls is too many "),	# 参数urls太多了
           21503: (SinaServerErrorNoRetry, "IP is null "),	# IP是空值
           21504: (SinaServerErrorNoRetry, "Url is null "),	# 参数url是空值

           21601: (SinaServerError, "Manage notice error, need auth "),	# 需要系统管理员的权限
           21602: (SinaServerErrorNoRetry, "Contains forbid world "),	# 含有敏感词
           21603: (SinaServerError, "Applications send notice over the restrictions "),	# 通知发送达到限制

           21701: (SinaServerError, "Manage remind error, need auth "),	# 提醒失败，需要权限
           21702: (SinaServerError, "Invalid category "),	# 无效分类
           21703: (SinaServerError, "Invalid status "),	# 无效状态码

           21901: (SinaServerErrorNoRetry, "Geo code input error "),	# 地理信息输入错误
           }

TOKEN_ERRORS = set([21314, 21315, 21316, 21317, 21318, 21327])


def getErrorFromCode(code, msg=None, method=None, tokennoretry=False):
    try:
        code = int(code)
        exc_info = ERRORS[code]
    except KeyError, err:
        print str(err)
        log.warning('Unknown error code: ' + str(code) + \
                        ' with msg: ' + msg if msg is not None else '' + \
                        ' when calling: ' + method if method is not None else '')
        return SinaServerError(code, msg, method)
    if msg is None:
        msg = exc_info[1]
    if method is None:
        method = ''
    if tokennoretry and code in TOKEN_ERRORS:
        exc_type = SinaServerErrorNoRetry
    else:
        exc_type = exc_info[0]
    return exc_type(code, msg, method)


def getErrorFromResponse(sina_response, tokennoretry=False):
    try:
        response = json_loads(sina_response)
    except:
        return None
    if 'error_code' not in response:
        log.warning('Unknown error in response: ' + repr(response))
        return None
    code = int(response['error_code'])
    if code not in ERRORS:
        log.warning('Unknown error code: ' + str(code) \
                        + ' in response: ' + repr(response))
    return getErrorFromCode(code, response.get('error'),
                            response.get('request'), tokennoretry)

