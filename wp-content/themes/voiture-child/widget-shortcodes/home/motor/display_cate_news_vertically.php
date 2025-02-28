<?php
function enqueue_motor_cate_news_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'display_motor_cate_news_vertically')) {
        // Register and enqueue the CSS file
        wp_enqueue_style(
            'cate-news-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/cate-news.css',
            array(),
            '1.0',
            'all'
        );
//     }
}
add_action('wp_enqueue_scripts', 'enqueue_motor_cate_news_css');

add_action('wp_ajax_motor_cate_news_pagination', 'motor_cate_news_pagination_handler');
add_action('wp_ajax_nopriv_motor_cate_news_pagination', 'motor_cate_news_pagination_handler');
function motor_cate_news_pagination_handler()
{
    if (!isset($_POST['page']) || !isset($_POST['category']) ||  !isset($_POST['page_url'])) {
        wp_send_json_error('Missing parameters');
    }

    $page = intval($_POST['page']);
    $category = sanitize_text_field($_POST['category']);
    $page_url = sanitize_text_field($_POST['page_url']);
    error_log("Page: $page, Category: $category");

    if (shortcode_exists('display_motor_cate_news_vertically')) {
        error_log("Shortcode 'display_motor_cate_news_vertically' is registered.");
    } else {
        error_log("Shortcode 'display_motor_cate_news_vertically' is NOT registered.");
    }

    ob_start();
    echo do_shortcode("[display_motor_cate_news_vertically category='$category' paged='$page' page_url='$page_url']");
    $response = ob_get_clean();

    wp_send_json_success($response);
}

function get_motor_term_by_name_taxonomy_description($term_name, $taxonomy, $description)
{
    $terms = get_terms(array(
        'taxonomy'   => $taxonomy,
        'name'       => $term_name,
        'hide_empty' => false,
//         'description__like' => 'Sub-category'
    ));

    if (! is_wp_error($terms) && ! empty($terms)) {
        foreach ($terms as $term) {
            // Verify the description manually since `description__like` doesn't exist natively
            if (stripos($term->description, $description) !== false) {
                return $term;
            }
        }
    }
  // If no subcategory matches, get the parent term's ID
        $parent_term = $terms[0]; // Assuming the first term matches the name (adjust if multiple results are possible)

        // Fetch "Others" subcategory for the parent term
        $others_terms = get_terms(array(
            'taxonomy'     => $taxonomy,
            'name'         => 'Others', // The name of the "Others" subcategory
            'hide_empty'   => false,
            'parent'       => $parent_term->term_id, // Ensure it's a subcategory of the parent term
        ));

        // Return "Others" subcategory if found
        return (!is_wp_error($others_terms) && !empty($others_terms)) ? $others_terms[0] : false;
}

function display_motor_cate_news_vertically_shortcode($atts)
{
    $atts = shortcode_atts(
        array(
            'category' => '',
            'posts_per_page' => 4,
            'paged' => 1, // Accept paged parameter for pagination
            'page_url' => '',
        ),
        $atts,
        'display_motor_cate_news_vertically'
    );

    global $DOMAIN_NAME;
    if ($atts['page_url'] != $DOMAIN_NAME && $atts['page_url'] != '') {
        return "";
    } else {
        wp_enqueue_script('cate-news-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/motor/js/cate-news-shortcode.js', array(), null, true);
    }


    $current_page = isset($atts['paged']) ? intval($atts['paged']) : 1;
    // $paged = isset($atts['paged']) ? intval($atts['paged']) : 1;
    $posts_per_page = $atts['posts_per_page'];
    $news_category = $atts['category'];
    $taxonomy = 'motorcycle-news-category';
    $description = 'Sub-category';

    $good_reads = get_motor_category_news_data($news_category, $description, $current_page);
//     $news_category_term = get_motor_term_by_name_taxonomy_description($news_category, $taxonomy, $description);
//     if ($news_category_term) {
//         $news_category_term_id = $news_category_term->term_id;
//         $serialized_value = '"' . $news_category_term_id . '"';

//         $cache_key = 'motor_cate_news_' . strtolower(str_replace(' ', '_', $news_category)) . '_' . $current_page;
// 		delete_transient($cache_key);
//         $good_reads = get_transient($cache_key);

//         if (false === $good_reads) {
//             $query_args = [
//                 'post_type'  => 'motorcycle-news',
//                 'meta_query' => [
//                     [
//                         'key'     => 'motorcycle-news-category',
//                         'value'   => $serialized_value,
//                         'compare' => 'LIKE',
//                     ],
// //                     [
// //                         'key'     => 'second_language',
// //                         'value'   => '',
// //                         'compare' => '==',
// //                     ]
//                 ],
//                 'posts_per_page' => $posts_per_page,
//                 'paged' => $current_page,
//             ];

//             $good_reads_data = new WP_Query($query_args);
//             if ($good_reads_data->have_posts()) {
//                 $good_reads = [];
//                 while ($good_reads_data->have_posts()) : $good_reads_data->the_post();
//                     $post_id = get_the_ID();
//                     $thumbnail_id = get_post_meta($post_id, '_thumbnail_id', true);
//                     $image_post = get_post($thumbnail_id);
//                     $image_guid = $image_post ? $image_post->guid : ''; // Handle missing image
//                     $post_author = get_the_author_meta('display_name', get_post_field('post_author', $post_id));
//                     $post_date = get_the_date('F j, Y', $post_id);

//                     $good_reads[] = [
//                         'id' => $post_id,
//                         'title' => get_the_title(),
//                         'link' => get_permalink(),
//                         'image_guid' => $image_guid,
//                         'author' => $post_author,
//                         'date' => $post_date,
//                     ];
//                 endwhile;
//                 set_transient($cache_key, $good_reads, HOUR_IN_SECONDS);
//             }
//         }

//         wp_reset_postdata();
//     }

    ob_start();
    // Add a unique class or ID for the container
    $container_id = 'cate-news-' . sanitize_title($news_category);

    if (!empty($good_reads)) { ?>
        <div class="cate-news-container" id="<?php echo $container_id; ?>">
            <?php
            $category_title = !empty($news_category) ? $news_category : 'ข่าวสาร';
            ?>
            <h2 class="good-head wa-title-text"><?php echo esc_html($category_title); ?></h2>
             <ul class="good-news-list">
                <?php
                $index = 0;
                foreach ($good_reads as $post_data) :
                    if ($index == 0) : ?>
                        <li class="first-news-item news-item-first">
                            <a href="<?php echo esc_url($post_data['link']); ?>">
                                <?php if ($post_data['thumbnail_url']) : ?>
                                    <div class="first-news-thumbnail">
                                        <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                                        <div class="first-news-content">
                                            <a href="<?php echo esc_url($post_data['link']); ?>">
                                                <?php echo esc_html(wp_trim_words($post_data['title'], 10, '...')); ?>
                                            </a>
                                            <span class="first-news-date"><?php //echo get_the_date(); 
                                                                            ?></span>
                                        </div>
                                    </div>
                                <?php endif; ?>
                            </a>
                        </li>
                    <?php else : ?>
				   <li class="good-news-item">
                        <a href="<?php echo esc_url($post_data['link']); ?>">
                            <?php if ($post_data['thumbnail_url']) : ?>
                                <div class="good-news-thumbnail">
                                    <div class="image-container">
                                        <img src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo $post_data['title']; ?>" />
                                    </div>
                                </div>
                            <?php endif; ?>
                            <div class="good-news-content col-md-12">
                                <a href="<?php echo esc_url($post_data['link']); ?>"><?php echo esc_html($post_data['title']); ?></a>
                                <div class="good-news-meta">
                                    <div class="name-date">
                                        <div class="author-single">
                                            <!-- <img class="author-img" src="<?php //echo esc_url($post_data['author_img']); 
                                                                                ?>" /> -->
                                            <span class="author-name letter-upper-case"><?php echo esc_html($post_data['author']); ?></span> <!-- Display the author -->
                                        </div>

                                        <span class="good-news-date"><?php echo esc_html($post_data['post_date']); ?></span> <!-- Display the date -->

                                    </div>
                                </div>
                            </div>
                        </a>
					    </li>
                <?php endif;
                    $index++;
                endforeach; ?>
            </ul>
            <div class="home-pagination">
                <div class="prev-page" data-page="<?php echo max(1, $current_page - 1); ?>" data-category="<?php echo esc_attr($news_category); ?>">
                    <svg width="16" height="16" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M15 18l-6-6 6-6"></path> <!-- Left arrow -->
                    </svg>
                </div>
                <div class="next-page" data-page="<?php echo $current_page + 1; ?>" data-category="<?php echo esc_attr($news_category); ?>">
                    <svg width="16" height="16" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M9 18l6-6-6-6"></path> <!-- Right arrow -->
                    </svg>
                </div>
            </div>
        </div>
        <style>
            .home-pagination {
                display: flex;
                flex-direction: row;
            }

            .home-pagination div {
                display: flex;
                justify-content: center;
                align-items: center;
            }

            .home-pagination .prev-page svg,
            .home-pagination .next-page svg {
                width: 42px;
                height: 39px;
                background: #f7f7f7;
                border: 1px solid #e5e5e5;
                display: flex;
                justify-content: center;
                align-items: center;
                cursor: pointer;
                padding: 10px;
                transition: all .2s;
            }

            .home-pagination .next-page svg {
                margin-left: 10px;
            }


            .home-pagination .next-page svg :hover {
                background: #e5e5e5;

            }

            /* Center the loader and change color to blue */
            .loader {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                text-align: center;
                font-size: 24px;
                color: #007bff;
                /* Blue color */
                z-index: 9999;
                /* Ensure it's on top */
            }

            .loader i {
                font-size: 2em;
                /* Increase spinner size */
            }

            .cate-news-container {
                position: relative;
                /* Ensure the container has positioning context for the loader */
            }
        </style>
    <?php } else { ?>
        <p>No news available.</p>
<?php }
    wp_reset_postdata();

    return ob_get_clean();
}

// change the shortcode name to 'display_motor_cate_news_vertically'
add_shortcode('display_motor_cate_news_vertically', 'display_motor_cate_news_vertically_shortcode');
