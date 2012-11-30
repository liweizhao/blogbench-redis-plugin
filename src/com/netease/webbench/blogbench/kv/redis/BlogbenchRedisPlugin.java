package com.netease.webbench.blogbench.kv.redis;

import com.netease.webbench.blogbench.BlogbenchPlugin;
import com.netease.webbench.blogbench.dao.BlogDaoFactory;
import com.netease.webbench.blogbench.dao.DataLoader;
import com.netease.webbench.blogbench.misc.BbTestOptions;
import com.netease.webbench.blogbench.misc.ParameterGenerator;
import com.netease.webbench.common.DbOptions;

public class BlogbenchRedisPlugin implements BlogbenchPlugin {
	public BlogDaoFactory daoFacory = new RedisBlogDaoFactory();
	
	public BlogbenchRedisPlugin() {
	}
	
	@Override
	public DataLoader getDataLoader(DbOptions dbOpt, BbTestOptions bbTestOpt, 
			ParameterGenerator parGen) throws Exception {
		return new RedisDataLoader(dbOpt, bbTestOpt, parGen, daoFacory);
	}

	@Override
	public BlogDaoFactory getBlogDaoFacory() throws Exception {
		// TODO Auto-generated method stub
		return daoFacory;
	}

	@Override
	public void validateOptions(DbOptions dbOpt, BbTestOptions bbTestOpt)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		
	}
}
