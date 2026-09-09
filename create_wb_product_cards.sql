-- WB tableListv6 商品卡片建表脚本
-- 列名与 wb_to_oss.py 导出的 CSV 表头（CANONICAL_COLS，32 列）一一对应
-- 注意：PostgreSQL 会把未加引号的标识符折叠为小写，
--       实际列名 nmID -> nmid、imtID -> imtid、meta_needUIN -> meta_needuin

CREATE TABLE wb_product_cards (
    nmID                             bigint        PRIMARY KEY,   -- 商品ID
    imtID                            bigint,                      -- imtID
    vendorCode                       text,                        -- 商家编码
    title                            text,                        -- 商品标题
    brand                            text,                        -- 品牌
    brandRaw                         text,                        -- 原始品牌名
    subject                          text,                        -- 品类名称
    colors                           text,                        -- 颜色列表（逗号拼接）
    updateAt                         timestamptz,                 -- 更新时间 ISO 8601，如 2026-08-16T06:45:58Z
    stocks                           integer,                     -- 总库存
    discount                         smallint,                    -- 折扣百分比
    discountSource                   smallint,                    -- 折扣来源
    feedbackRating                   numeric(5,2),                -- 评价星级
    feedbacks                        text,                        -- 评价信息 JSON 字符串
    externalBan                      text,                        -- 封禁信息 JSON 字符串
    sizes_count                      integer,                     -- 尺码数量
    sizes_detail                     text,                        -- 尺码明细 techSize(price=..,skus=..)
    mediaFiles_count                 integer,                     -- 媒体文件数量
    tags                             text,                        -- 标签列表（逗号拼接）
    meta_needKiz                     boolean,                     -- 是否需要 KIZ（诚实标志）
    meta_needUIN                     boolean,                     -- 是否需要 UIN
    rating                           numeric(5,2),                -- 卡片评分
    isCardRated                      boolean,                     -- 卡片是否已评分
    err_characteristics              text,                        -- 特征校验错误
    err_title                        text,                        -- 标题校验错误
    err_description                  text,                        -- 描述校验错误
    err_image                        text,                        -- 图片校验错误
    err_brand                        text,                        -- 品牌校验错误
    hasPaidOptions_hasPhotoTags      boolean,                     -- 付费选项：照片标签
    hasPaidOptions_hasRichContent    boolean,                     -- 付费选项：富媒体内容
    hasPaidOptions_hasAutoplayVideo  boolean,                     -- 付费选项：自动播放视频
    hasPaidOptions_hasClientTryOn    boolean                      -- 付费选项：客户试穿
);

COMMENT ON TABLE wb_product_cards IS 'WB tableListv6 商品卡片快照（CSV 32 列，与 wb_to_oss.py CANONICAL_COLS 对应）';
