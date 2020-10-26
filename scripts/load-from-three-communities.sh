for community_id in 45491419 149279263б 85443458; do
  python -m suitable-hat load --community-id $community_id --cache-path $community_id.pkl
done
