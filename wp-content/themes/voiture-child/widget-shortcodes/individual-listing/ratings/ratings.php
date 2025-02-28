<?php
function enqueue_custom_styles()
{
    wp_enqueue_style('ratings-style', get_stylesheet_directory_uri() . '/widget-shortcodes/individual-listing/ratings/ratings.css');
}
add_action('wp_enqueue_scripts', 'enqueue_custom_styles');

add_shortcode('reviews', 'display_reviews');
function display_reviews($atts)
{
    require_once(get_stylesheet_directory() . '/json-ld/review-json-ld.php');

    global $wpdb;

    $global_listing_post_data = get_listing_from_query_vars();
    if (!$global_listing_post_data || !is_array($global_listing_post_data)) {
        return;
    }

    $post = $global_listing_post_data['post'];
    $variant_posts = $global_listing_post_data['variant_posts'];
    if (empty($variant_posts)) {
        return;
    }

    $total_user_reviews = 0;
    $average_total_rating = 0;
    $user_reviews = [];


    foreach ($variant_posts as $variant_post) {
        // get all User-Review posts whose post_parent is the current variant post
        $user_review_posts = get_posts(array(
            'post_parent' => $variant_post->ID,
            'post_type' => 'user-review',
            'posts_per_page' => -1
        ));

        // get user review 
        $user_review_post_ids = wp_list_pluck($user_review_posts, 'ID');
        $all_user_reviews_post_meta_query = $wpdb->prepare(
            "SELECT post_id, meta_key, meta_value FROM $wpdb->postmeta WHERE post_id IN (%s)",
            implode(',', $user_review_post_ids)
        );
        $all_user_reviews_post_meta = $wpdb->get_results($all_user_reviews_post_meta_query);

        // group the meta data by post_id
        $grouped_user_reviews_post_meta = [];
        foreach ($all_user_reviews_post_meta as $meta) {
            $grouped_user_reviews_post_meta[$meta->post_id][$meta->meta_key] = $meta->meta_value;
        }

        foreach ($user_review_posts as $user_review_post) {
            $total_user_reviews += 1;
            $user_review_post_meta = $grouped_user_reviews_post_meta[$user_review_post->ID] ?? null;
            if ($user_review_post_meta) {
                $average_total_rating += $user_review_post_meta['total_score'];
                $user_id = $user_review_post_meta['user'];
                $user = get_user_by('ID', $user_id);
                if ($user) {
                    $user_reviews[] = array(
                        'user_name' => $user->display_name,
                        'date' => $user_review_post_meta['date'] ?? '',
                        'total_score' => $user_review_post_meta['total_score'],
                        'variant_name' => $variant_post->post_title,
                        'pros' => $user_review_post_meta['pros'],
                        'cons' => $user_review_post_meta['cons']
                    );
                }
            }
        }
    }

    if ($total_user_reviews == 0) {
        return;
    }

    // order by date desc and slice the first 2
    usort($user_reviews, function ($a, $b) {
        return strtotime($b['date']) - strtotime($a['date']);
    });
    $user_reviews = array_slice($user_reviews, 0, 2);
    $average_total_rating = round($average_total_rating / $total_user_reviews, 1);

    if ($average_total_rating > 4) {
        $rating_text = 'Excellent';
    } else if ($average_total_rating > 3) {
        $rating_text = 'Good';
    } else if ($average_total_rating > 2) {
        $rating_text = 'Average';
    } else {
        $rating_text = 'Bad';
    }

    generate_reviews_json_ld($post->post_title, $user_reviews);

?>
    <div class="user-reviews">
        <div class="average-rating">
            <h2 class="wa-title-text"><?php echo $post->post_title; ?> User Reviews</h2>
            <div class="rating-score">
                <span class="score"><?php echo $average_total_rating ?></span>

                <div>
                    <div class="rating-text"><?php echo $rating_text ?></div>
                    <!-- Add star images or icons here -->
                    <div class="stars">
                        <?php
                        $nearest_int = round($average_total_rating);
                        echo str_repeat('★', $nearest_int) . str_repeat('☆', 5 - $nearest_int);
                        ?>
                        <!-- ★★★★☆	 -->
                    </div>

                </div>
                <p>Based on <?php echo $total_user_reviews ?> Reviews</p>
            </div>
        </div>

        <div class="reviews-list">
            <?php foreach ($user_reviews as $review): ?>
                <div class="review-card">
                    <div class="review-header">
                        <span>
                            <span class="flex-column">
                                <span class="user-id"><?php echo $review['user_name']; ?></span>
                                <span class="review-date"><?php echo date("d.m.Y", strtotime($review['date'])); ?></span>
                            </span>
                        </span>
                        <div>
                            <div class="review-score">
                                <?php echo $review['total_score']; ?>
                            </div>

                            <div class="stars-ratting">
                                ★★★★☆
                            </div>
                        </div>
                    </div>
                    <div class="review-variant">
                        <?php echo $review['variant_name']; ?>
                    </div>
                    <div class="pros-cons">
                        <div class="pros">
                            <span class="label">Pros</span>
                            <p><?php echo $review['pros']; ?></p>
                        </div>
                        <div class="cons">
                            <span class="label">Cons</span>
                            <p><?php echo $review['cons']; ?></p>
                        </div>
                    </div>
                    <!-- <a href="#" class="read-more">Read More</a> -->
                </div>
            <?php endforeach; ?>
        </div>
    </div>
<?php
    wp_reset_postdata();
}
