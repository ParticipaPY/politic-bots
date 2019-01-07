db.getCollection("tweets").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { "author_party": "pdp", "flag.partido_politico.pdp": NumberInt(0), "relevante": NumberInt(1) }
		},

		// Stage 2
		{
			$project: {
			    "id": "$tweet_obj.id",
			    "tweet": "$tweet_obj.full_text",
			    "quote": "$tweet_obj.is_quote_status",
			    "retweeted_status":  { $cond: { if: { $eq: ["$tweet_obj.retweeted_status", null]}, then: "false", else: "true" } },
			    "reply": { $cond: { if: { $eq: ["$tweet_obj.in_reply_to_status_id", null]}, then: "false", else: "true" } },
			    "datetime": "$tweet_py_datetime", 
			    "author_screen_name": "$tweet_obj.user.screen_name",
			    "author_description": "$tweet_obj.user.description",
			    "author_party": "$author_party",
			    "sentiment": { $ifNull: [ "$sentimiento.score", "Unspecified" ] },
			    "anr_flag" : "$flag.partido_politico.anr",
			    "pdp_flag" : "$flag.partido_politico.pdp",
			    "ganar_flag" : "$flag.partido_politico.ganar",
			    "plra_flag" : "$flag.partido_politico.plra",
			    "ppq_flag" : "$flag.partido_politico.ppq",
			    "hagamos_flag" : "$flag.partido_politico.hagamos",
			    "fg_flag" : "$flag.partido_politico.fg"
			}
		},

		// Stage 3
		{
			$sort: {
			  "datetime": -1
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
