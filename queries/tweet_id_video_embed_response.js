db.getCollection("tweets").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { "video_embed_url.is_video": NumberInt(1), "video_embed_url.is_false_positive": { $exists: false } }
		},

		// Stage 2
		{
			$project: {
			    tweet: "$tweet_obj.id_str",
			    response: "$video_embed_url.is_video_response"
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
