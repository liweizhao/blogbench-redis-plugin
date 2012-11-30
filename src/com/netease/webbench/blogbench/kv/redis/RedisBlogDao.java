/**
  * Copyright (c) <2011>, <NetEase Corporation>
  * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *    2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *    3. Neither the name of the <ORGANIZATION> nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.netease.webbench.blogbench.kv.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.netease.webbench.blogbench.dao.BlogDAO;
import com.netease.webbench.blogbench.model.Blog;
import com.netease.webbench.blogbench.model.BlogIdWithTitle;
import com.netease.webbench.blogbench.model.BlogInfoWithPub;
import com.netease.webbench.blogbench.model.SiblingPair;
import com.netease.webbench.common.DynamicArray;

/**
 * Redis data access layer
 * 
 * @author LI WEIZHAO
 */
public class RedisBlogDao implements BlogDAO {
	public static final String ID_FIELD = "ID";
	public static final String UID_FIELD = "UserID";
	public static final String TITLE_FIELD = "Title";
	public static final String ABS_FIELD = "Abstract";
	public static final String ALLOWVIEW_FIELD = "AllowView";
	public static final String PTIME_FIELD = "PublishTime";
	public static final String ACCESS_FIELD = "AccessCount";
	public static final String COMMENT_FIELD = "CommentCount";
	public static final String CONTENT_FIELD = "Content";
	
	private Jedis jedis;
	
	public RedisBlogDao(String host, int port) {
		this.jedis = new Jedis(host, port, 0);
	}
	
	private Map<String, String> blog2Map(Blog blog) {		
		Map<String, String> map = new HashMap<String, String>();
		map.put(ID_FIELD, String.format("%d",blog.getId()));
		map.put(UID_FIELD, String.format("%d", blog.getUid()));
		map.put(TITLE_FIELD, blog.getTitle());
		map.put(ABS_FIELD, blog.getAbs());
		map.put(ALLOWVIEW_FIELD, String.format("%d", blog.getAllowView()));
		map.put(PTIME_FIELD, String.format("%d",blog.getPublishTime()));
		map.put(ACCESS_FIELD, String.format("%d",blog.getAccessCount()));
		map.put(COMMENT_FIELD, String.format("%d",blog.getCommentCount()));
		map.put(CONTENT_FIELD, blog.getCnt());
		return map;
	}
	
	private Blog map2Blog(Map<String, String> map) {
		Blog blog = new Blog();
		blog.setId(Long.parseLong(map.get(ID_FIELD)));
		blog.setUid(Long.parseLong(map.get(UID_FIELD)));
		blog.setTitle(map.get(TITLE_FIELD));
		blog.setAbs(map.get(ABS_FIELD));
		blog.setAllowView(Integer.parseInt(map.get(ALLOWVIEW_FIELD)));
		blog.setPublishTime(Long.parseLong(map.get(PTIME_FIELD)));
		blog.setAccessCount(Integer.parseInt(map.get(ACCESS_FIELD)));
		blog.setCommentCount(Integer.parseInt(map.get(COMMENT_FIELD)));
		blog.setCnt(map.get(CONTENT_FIELD));
		return blog;
	}

	@Override
	public Blog selectBlog(long blogId, long uId) throws IOException {
		Map<String, String> map = jedis.hgetAll(KeyUtils.bid(blogId));
		return map != null ? map2Blog(map) : null;
	}

	@Override
	public List<Long> selBlogList(long uId) throws IOException {
		Set<String> res = jedis.zrevrangeByScore(KeyUtils.blogs(uId), 
				"+inf", "-inf", 0, 10);
		if (res != null && res.size() > 0) {
			List<Long> rl = new ArrayList<Long>(res.size());
			for (String v : res)
				Long.parseLong(v);
			return rl;
		}
		return null;
	}
	
	private BlogIdWithTitle getSibling(long uId, long time, boolean forward) {
		Set<String> res = null;
		res = forward ? jedis.zrangeByScore(KeyUtils.blogs(uId), 
				String.format("%d", time), "+inf", 0, 1) 
				: jedis.zrevrangeByScore(KeyUtils.blogs(uId), 
						String.format("%d", time), "-inf", 0, 1);
		if (res != null && res.size() > 0) {
			Iterator<String> it = res.iterator();
			long blogId = Long.parseLong(it.next());
			String title = jedis.hget(KeyUtils.bid(blogId), PTIME_FIELD);
			return title != null ? new  BlogIdWithTitle(blogId, uId, title) : null;
		}
		return null;
	}
	
	@Override
	public SiblingPair selSiblings(
			long uId, long time) throws IOException {
		return new SiblingPair(getSibling(uId, time, false), 
				getSibling(uId, time, true));
	}

	@Override
	public DynamicArray<BlogInfoWithPub> selAllBlogIds() throws IOException {
		Set<String> keys = jedis.keys(KeyUtils.bid("*"));
		DynamicArray<BlogInfoWithPub> arr = new DynamicArray<BlogInfoWithPub>(keys.size());
		for (String key : keys) {			
			Blog blog = map2Blog(jedis.hgetAll(key));	
			arr.append(new BlogInfoWithPub(blog.getId(), blog.getUid(), 
					blog.getPublishTime()));
		}
		return arr;
	}

	@Override
	public long selBlogNums() throws IOException {
		return jedis.keys(KeyUtils.bid("*")).size();
	}

	@Override
	public int insertBlog(Blog b) throws Exception {	
		Transaction t = jedis.multi();
		// add blog
		t.hmset(KeyUtils.bid(b.getId()), blog2Map(b));
		// add link
		t.zadd(KeyUtils.blogs(b.getUid()), b.getPublishTime(), 
				String.format("%d", b.getId()));
		t.exec();
		return 1;
	}

	@Override
	public int updateAccess(long blogId, long uId) throws Exception {
		jedis.hincrBy(KeyUtils.bid(blogId), ACCESS_FIELD, 1);
		return 1;
	}

	@Override
	public int updateComment(long blogId, long uId) throws Exception {
		jedis.hincrBy(KeyUtils.bid(blogId), COMMENT_FIELD, 1);
		return 1;
	}

	@Override
	public int updateBlog(Blog blog) throws Exception {
		Transaction t = jedis.multi();
		t.hset(KeyUtils.bid(blog.getId()), CONTENT_FIELD, blog.getCnt());
		t.hset(KeyUtils.bid(blog.getId()), PTIME_FIELD, 
				String.format("%d", blog.getPublishTime()));
		t.zadd(KeyUtils.blogs(blog.getUid()), blog.getPublishTime(), 
				String.format("%d", blog.getId()));
		t.exec();
		return 1;
	}

	@Override
	public int batchInsert(List<Blog> blogList) throws Exception {
		Transaction t = jedis.multi();
		for (Blog b : blogList) {
			// add blog
			t.hmset(KeyUtils.bid(b.getId()), blog2Map(b));
			// add link
			t.zadd(KeyUtils.blogs(b.getUid()), (double)b.getPublishTime(), 
					String.format("%d", b.getId()));
		}
		t.exec();
		return blogList.size();
	}

	@Override
	public void close() {
		if (jedis != null)
			jedis.disconnect();
	}

}
