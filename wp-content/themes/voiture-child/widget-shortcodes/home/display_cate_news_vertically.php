<?php
function enqueue_cate_news_css()
{
    global $post;
//     if (isset($post->post_content) && has_shortcode($post->post_content, 'display_cate_news_vertically')) {
        // Register and enqueue the CSS file
        wp_enqueue_style(
            'cate-news-style',
            get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/cate-news.css',
            array(),
            '1.0',
            'all'
        );
//     }

    // import display_cate_news_vertically.css
    wp_enqueue_style('display_cate_news_vertically', get_stylesheet_directory_uri() . '/widget-shortcodes/home/css/display_cate_news_vertically.css');
}
add_action('wp_enqueue_scripts', 'enqueue_cate_news_css');

add_action('wp_ajax_cate_news_pagination', 'cate_news_pagination_handler');
add_action('wp_ajax_nopriv_cate_news_pagination', 'cate_news_pagination_handler');
function cate_news_pagination_handler()
{
    if (!isset($_POST['page']) || !isset($_POST['category']) ||  !isset($_POST['page_url'])) {
        wp_send_json_error('Missing parameters');
    }

    $page = intval($_POST['page']);
    $category = sanitize_text_field($_POST['category']);
    $page_url = sanitize_text_field($_POST['page_url']);
    error_log("Page: $page, Category: $category");

    if (shortcode_exists('display_cate_news_vertically')) {
        error_log("Shortcode 'display_cate_news_vertically' is registered.");
    } else {
        error_log("Shortcode 'display_cate_news_vertically' is NOT registered.");
    }

    ob_start();
    echo do_shortcode("[display_cate_news_vertically category='$category' paged='$page' page_url='$page_url']");
    $response = ob_get_clean();

    wp_send_json_success($response);
}

function get_term_by_name_taxonomy_description($term_name, $taxonomy, $description)
{

    $terms = get_terms(array(
        'taxonomy'   => $taxonomy,
        'name'       => $term_name,
        'hide_empty' => false,
        'parent' => 0,
        'description__like' => $description
    ));

    if (! is_wp_error($terms) && ! empty($terms)) {
        foreach ($terms as $term) {
            // Verify the description manually since `description__like` doesn't exist natively
            if (stripos($term->description, $description) !== false) {
                return $term;
            }
        }
    }

    return false;
}

function get_term_by_name_taxonomy_description_id($term_name, $taxonomy, $description)
{
    global $wpdb;
    $query = $wpdb->prepare(
        "
    SELECT t.*, tt.*
    FROM {$wpdb->terms} AS t
    INNER JOIN {$wpdb->term_taxonomy} AS tt
    ON t.term_id = tt.term_id
    WHERE tt.taxonomy = %s
    AND t.name = %s
    AND tt.description = %s ",
        $taxonomy,
        $term_name,
        $description
    );

    $terms = $wpdb->get_results($query);

    if (! is_wp_error($terms) && ! empty($terms)) {
        foreach ($terms as $term) {
            // Verify the description manually since `description__like` doesn't exist natively
            if (stripos($term->description, $description) !== false) {
                return $term;
            }
        }
    }

    return false;
}

function display_cate_news_vertically_shortcode($atts)
{
	enqueue_cate_news_css();
    $atts = shortcode_atts(
        array(
            'category' => 'Good Reads',
            'posts_per_page' => 4,
            'paged' => 1, // Accept paged parameter for pagination
            'page_url' => '',
        ),
        $atts,
        'display_cate_news_vertically'
    );

    $home_page = home_url().'/';
    if ($atts['page_url'] != $home_page && $atts['page_url'] != '') {
        return "";
    } else {
        wp_enqueue_script('cate-news-script', get_stylesheet_directory_uri() . '/widget-shortcodes/home/js/cate-news-shortcode.js', array(), null, true);
    }


    $current_page = isset($atts['paged']) ? intval($atts['paged']) : 1;
    // $paged = isset($atts['paged']) ? intval($atts['paged']) : 1;
    $posts_per_page = $atts['posts_per_page'];
    $news_category = $atts['category'];
    $taxonomy = 'news-category';
    $description = 'sub-category';

    $good_reads = get_category_news_data($news_category, $description, $current_page);
    // $good_reads = fetch_category_news_from_db([
    //     'category' => $news_category,
    //     'description' => $description,
    //     'page' => $current_page
    // ]);

    ob_start();
    // Add a unique class or ID for the container
    $container_id = 'cate-news-' . sanitize_title($news_category);

    if (!empty($good_reads)) { ?>
        <div class="cate-news-container" id="<?php echo $container_id; ?>">
            <h2 class="good-head wa-title-text"><?php echo $news_category; ?></h2>
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
    <?php } else { ?>
        <p></p>
<?php }
    wp_reset_postdata();

    return ob_get_clean();
}

// change the shortcode name to 'display_cate_news_vertically'
add_shortcode('display_cate_news_vertically', 'display_cate_news_vertically_shortcode');
