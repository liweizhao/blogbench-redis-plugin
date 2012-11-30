package com.netease.webbench.blogbench.kv.redis;

import com.netease.webbench.blogbench.dao.BlogDAO;
import com.netease.webbench.blogbench.dao.BlogDaoFactory;
import com.netease.webbench.blogbench.misc.BbTestOptions;
import com.netease.webbench.common.DbOptions;

public class RedisBlogDaoFactory implements BlogDaoFactory {

	@Override
	public BlogDAO getBlogDao(DbOptions dbOpt, BbTestOptions bbTestOpt)
			throws Exception {
		return new RedisBlogDao(dbOpt.getHost(), dbOpt.getPort());
	}

}
