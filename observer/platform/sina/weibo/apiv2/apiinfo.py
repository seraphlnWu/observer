# coding=utf8

_PAGE_PAGE = {
    'key': 'page',
    'init': 1,
    'incr': 1,
}


_PAGE_CURSOR = {
    'key': 'cursor',
    'init': 0,
    'incr': 'next_cursor',
}


API_INFOS = {
    'statuses__public_timeline': {
        'default': {'count': 200},
        'payload': 'statuses',
    }, #获取最新的公共微博 DONE
    'statuses__friends_timeline': {}, #获取当前登录用户及其所关注用户的最新微博
    'statuses__home_timeline': {}, #获取当前登录用户及其所关注用户的最新微博
    'statuses__friends_timeline__ids': {}, #获取当前登录用户及其所关注用户的最新微博的ID
    'statuses__user_timeline':
        { 'default':
              { 'count': 200 },
          'payload': 'statuses',
          'page': _PAGE_PAGE,
          }, #获取用户发布的微博 DONE
    'statuses__user_timeline__ids': {}, #获取用户发布的微博的ID
    'statuses__repost_timeline':
        { 'default':
              { 'count': 200, },
          'payload': 'reposts',
          'page': _PAGE_PAGE,
          }, #返回一条原创微博的最新转发微博 DONE
    'statuses__repost_timeline__ids': {}, #获取一条原创微博的最新转发微博的ID
    'statuses__repost_by_me': {}, #返回用户转发的最新微博
    'statuses__mentions':
        { 'default':
              { 'count': 200 },
          'payload': 'statuses',
          'page': _PAGE_PAGE,
          }, #获取@当前用户的最新微博 DONE
    'statuses__mentions__ids': {}, #获取@当前用户的最新微博的ID
    'statuses__bilateral_timeline': {}, #获取双向关注用户的最新微博
    'statuses__show':
        { 'payload_type': 'dict',
          }, #根据ID获取单条微博信息 DONE
    'statuses__querymid': {}, #通过id获取mid
    'statuses__queryid': {}, #通过mid获取id
    'statuses__hot__repost_daily': {}, #按天返回热门转发榜
    'statuses__hot__repost_weekly': {}, #按周返回热门转发榜
    'statuses__hot__comments_daily': {}, #按天返回当前用户关注人的热门微博评论榜
    'statuses__hot__comments_weekly': {}, #按周返回热门评论榜
    'statuses__count':
        { 'payload_type': 'dict',
        }, #批量获取指定微博的转发数评论数 DONE
    'statuses__show_batch':
        { 'payload': 'statues',
          }, #Undocumented API DONE
    'emotions': {}, #获取官方表情
    'statuses__repost': {}, #转发一条微博信息
    'statuses__destroy': {}, #删除微博信息
    'statuses__update': {}, #发布一条微博信息
    'statuses__upload': {}, #上传图片并发布一条微博
    'statuses__upload_url_text': {}, #发布一条微博同时指定上传的图片或图片url
    'comments__show':
        { 'default':
              { 'count': 50, },
          'payload': 'comments',
          'page': _PAGE_PAGE,
          }, #获取某条微博的评论列表 DONE
    'comments__by_me':
        { 'default':
              { 'count': 200 },
          'payload': 'comments',
          'page': _PAGE_PAGE,
          }, #我发出的评论列表 DONE
    'comments__to_me':
        { 'default':
              { 'count': 200 },
          'payload': 'comments',
          'page': _PAGE_PAGE,
          }, #我收到的评论列表 DONE
    'comments__timeline': {}, #获取用户发送及收到的评论列表
    'comments__mentions': {}, #获取@到我的评论
    'comments__show_batch': {}, #批量获取评论内容 DONE
    'comments__create': {}, #评论一条微博
    'comments__destroy': {}, #删除一条评论
    'comments__destroy_batch': {}, #批量删除评论
    'comments__reply': {}, #回复一条评论
    'users__show':
        { 'payload_type': 'dict',
          }, #获取用户信息 DONE
    'users__domain_show': {}, #通过个性域名获取用户信息
    'users__counts': {}, #批量获取用户的粉丝数、关注数、微博数 DONE
    'friendships__friends': {}, #获取用户的关注列表
    'friendships__friends__in_common': {}, #获取共同关注人列表
    'friendships__friends__bilateral': {}, #获取双向关注列表
    'friendships__friends__bilateral__ids': {}, #获取双向关注UID列表
    'friendships__friends__ids':
        { 'default':
              { 'count': 5000},
          'payload': 'ids',
          'page': _PAGE_CURSOR,
          }, #获取用户关注对象UID列表
    'friendships__followers':
        { 'default':
              { 'count': 200 },
          'payload': 'users',
          'page': _PAGE_CURSOR,
          }, #获取用户粉丝列表 DONE
    'friendships__followers__ids':
        { 'default':
              {'count': 5000},
          'payload': 'ids',
          'page': _PAGE_CURSOR,
          }, #获取用户粉丝UID列表 DONE
    'friendships__followers__active': {}, #获取用户优质粉丝列表
    'friendships__friends_chain__followers': {}, #获取我的关注人中关注了指定用户的人
    'friendships__show':
        { 'payload_type': 'dict',
          }, #获取两个用户关系的详细情况 DONE
    'friendships__create': {}, #关注某用户
    'friendships__destroy': {}, #取消关注某用户
    'friendships__remark__update': {}, #更新关注人备注
    'account__get_privacy': {}, #获取隐私设置信息
    'account__profile__school_list': {}, #获取所有学校列表
    'account__rate_limit_status': {}, #获取当前用户API访问频率限制
    'account__get_uid': {}, #OAuth授权之后获取用户UID（作用相当于旧版接口的 account__verify_credentials）
    'account__end_session': {}, #退出登录
    'favorites': {}, #获取当前用户的收藏列表
    'favorites__ids': {}, #获取当前用户的收藏列表的ID
    'favorites__show': {}, #获取单条收藏信息
    'favorites__by_tags': {}, #获取当前用户某个标签下的收藏列表
    'favorites__tags': {}, #当前登录用户的收藏标签列表
    'favorites__by_tags__ids': {}, #获取当前用户某个标签下的收藏列表的ID
    'favorites__create': {}, #添加收藏
    'favorites__destroy': {}, #删除收藏
    'favorites__destroy_batch': {}, #批量删除收藏
    'favorites__tags__update': {}, #更新收藏标签
    'favorites__tags__update_batch': {}, #更新当前用户所有收藏下的指定标签
    'favorites__tags__destroy_batch': {}, #删除当前用户所有收藏下的指定标签
    'trends': {}, #获取某人话题
    'trends__status':
        { 'default' :
              { 'count': 10, },
          'payload': 'statuses',
          'page': _PAGE_PAGE,
          }, # Used by wubin. have no offical document. DONE
    'trends__is_follow': {}, #是否关注某话题
    'trends__hourly': {}, #返回最近一小时内的热门话题
    'trends__daily': {}, #返回最近一天内的热门话题
    'trends__weekly': {}, #返回最近一周内的热门话题
    'trends__follow': {}, #关注某话题
    'trends__destroy': {}, #取消关注的某一个话题
    'tags':
        { 'default':
              { 'count': 20, },
          'page': _PAGE_PAGE,
          }, #返回指定用户的标签列表 DONE
    'tags__tags_batch': {}, #批量获取用户标签
    'tags__suggestions': {}, #返回系统推荐的标签列表
    'tags__create': {}, #添加用户标签
    'tags__destroy': {}, #删除用户标签
    'tags__destroy_batch': {}, #批量删除用户标签
    'register__verify_nickname': {}, #验证昵称是否可用
    'search__suggestions__users': {}, #搜用户搜索建议
    'search__suggestions__statuses': {}, #搜微博搜索建议
    'search__suggestions__schools': {}, #搜学校搜索建议
    'search__suggestions__companies': {}, #搜公司搜索建议
    'search__suggestions__apps': {}, #搜应用搜索建议
    'search__suggestions__at_users': {}, #@联想搜索
    'search__topics':
        { 'default':
              {'count':200, },
          'payload': 'statuses',
          'page': _PAGE_PAGE,
        }, #搜索某一话题下的微博 DONE
    'suggestions__users__hot': {}, #获取系统推荐用户
    'suggestions__users__may_interested': {}, #获取用户可能感兴趣的人
    'suggestions__users__by_status': {}, #根据微博内容推荐用户
    'suggestions__statuses__hot': {}, #获取微博精选推荐
    'suggestions__statuses__reorder': {}, #主Feed微博按兴趣推荐排序
    'suggestions__statuses__reorder__ids': {}, #主Feed微博按兴趣推荐排序的微博ID
    'suggestions__favorites__hot': {}, #热门收藏
    'suggestions__users__not_interested': {}, #不感兴趣的人
    'remind__unread_count': {}, #获取某个用户的各种消息未读数
    'remind__set_count': {}, #对当前登录用户某一种消息未读数进行清零
    'short_url__shorten':
        { 'payload': 'urls',
          }, #长链转短链 DONE
    'short_url__expand': {}, #短链转长链
    'short_url__clicks': {}, #获取短链接的总点击数
    'short_url__referers': {}, #获取一个短链接点击的referer来源和数量
    'short_url__locations': {}, #获取一个短链接点击的地区来源和数量
    'short_url__share__counts': {}, #获取短链接在微博上的微博分享数
    'short_url__share__statuses':
        { 'default':
              { 'count': 200, },
          'payload': 'share_statuses',
          'page': _PAGE_PAGE,
          }, #获取包含指定单个短链接的最新微博内容 DONE
    'short_url__comment__counts': {}, #获取短链接在微博上的微博评论数
    'short_url__comment__comments': {}, #获取包含指定单个短链接的最新微博评论
    'short_url__info': {}, #批量获取短链接的富内容信息
    'notification__send': {}, #给一个或多个用户发送一条新的状态通知
    'common__code_to_location': {}, #通过地址编码获取地址名称
    'common__get_city': {}, #获取城市列表
    'common__get_province': {}, #获取省份列表
    'common__get_country': {}, #获取国家列表
    'common__get_timezone': {}, #获取时区配置表
    'place__public_timeline': {}, #获取公共的位置动态
    'place__friends_timeline': {}, #获取用户好友的位置动态
    'place__user_timeline': {}, #获取某个用户的位置动态
    'place__poi_timeline': {}, #获取某个位置地点的动态
    'place__nearby_timeline': {}, #获取某个位置周边的动态
    'place__statuses__show': {}, #获取动态的详情
    'place__users__show': {}, #获取LBS位置服务内的用户信息
    'place__users__checkins': {}, #获取用户签到过的地点列表
    'place__users__photos': {}, #获取用户的照片列表
    'place__users__tips': {}, #获取用户的点评列表
    'place__users__todos': {}, #获取用户的todo列表
    'place__pois__show': {}, #获取地点详情
    'place__pois__users': {}, #获取在某个地点签到的人的列表
    'place__pois__tips': {}, #获取地点点评列表
    'place__pois__photos': {}, #获取地点照片列表
    'place__pois__search': {}, #按省市查询地点
    'place__pois__category': {}, #获取地点分类
    'place__nearby__pois': {}, #获取附近地点
    'place__nearby__users': {}, #获取附近发位置微博的人
    'place__nearby__photos': {}, #获取附近照片
    'place__nearby_users__list': {}, #获取附近的人
    'place__pois__create': {}, #添加地点
    'place__pois__add_checkin': {}, #签到
    'place__pois__add_photo': {}, #添加照片
    'place__pois__add_tip': {}, #添加点评
    'place__pois__add_todo': {}, #添加todo
    'place__nearby_users__create': {}, #用户添加自己的位置
    'place__nearby_users__destroy': {}, #用户删除自己的位置
    'location__base__get_map_image': {}, #生成一张静态的地图图片
    'location__geo__ip_to_geo': {}, #根据IP地址返回地理信息坐标
    'location__geo__address_to_geo': {}, #根据实际地址返回地理信息坐标
    'location__geo__geo_to_address': {}, #根据地理信息坐标返回实际地址
    'location__geo__gps_to_offset': {}, #根据GPS坐标获取偏移后的坐标
    'location__geo__is_domestic': {}, #判断地理信息坐标是否是国内坐标
    'location__pois__show_batch': {}, #批量获取POI点的信息
    'location__pois__search__by_location': {}, #根据关键词按地址位置获取POI点的信息
    'location__pois__search__by_geo': {}, #根据关键词按坐标点范围获取POI点的信息
    'location__pois__search__by_area': {}, #根据关键词按矩形区域获取POI点的信息
    'location__pois__add': {}, #提交一个新增的POI点信息
    'location__mobile__get_location': {}, #根据移动基站WIFI等数据获取当前位置信息
    'location__line__drive_route': {}, #根据起点与终点数据查询自驾车路线信息
    'location__line__bus_route': {}, #根据起点与终点数据查询公交乘坐路线信息
    'location__line__bus_line': {}, #根据关键词查询公交线路信息
    'location__line__bus_station': {}, #根据关键词查询公交站点信息
    'location__citycode': {}, #城市代码对应表
    'location__citycode_bus': {}, #公交城市代码表
    'location__category': {}, #分类代码对应表
    'location__error2': {}, #地理位置信息接口错误代码及解释
    'oauth2__authorize': {}, #请求用户授权Token
    'oauth2__access_token': {}, #获取授权过的Access Token
    'oauth2__get_oauth2_token': {}, #OAuth1.0的Access Token更换至OAuth2.0的Access Token
    }
