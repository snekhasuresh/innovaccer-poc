<?php
function enqueue_ev_cate_news_css()
{
    // Register and enqueue the CSS file
    wp_enqueue_style(
        'cate-news-style',
        get_stylesheet_directory_uri() . '/widget-shortcodes/ev/css/ev-cate-news.css',
        array(),
        '1.0',
        'all'
    );
}

add_action('wp_enqueue_scripts', 'enqueue_ev_cate_news_css');

add_action('wp_ajax_ev_cate_news_pagination', 'ev_cate_news_pagination_handler');
add_action('wp_ajax_nopriv_ev_cate_news_pagination', 'ev_cate_news_pagination_handler');
function ev_cate_news_pagination_handler()
{
    if (!isset($_POST['page']) || !isset($_POST['category']) ||  !isset($_POST['page_url'])) {
        wp_send_json_error('Missing parameters');
    }

    $page = intval($_POST['page']);
    $category = sanitize_text_field($_POST['category']);
    $page_url = sanitize_text_field($_POST['page_url']);
    error_log("Page: $page, Category: $category");

    if (shortcode_exists('display_ev_cate_news_vertically')) {
        error_log("Shortcode 'display_ev_cate_news_vertically' is registered.");
    } else {
        error_log("Shortcode 'display_ev_cate_news_vertically' is NOT registered.");
    }

    ob_start();
    echo do_shortcode("[display_ev_cate_news_vertically category='$category' paged='$page' page_url='$page_url']");
    $response = ob_get_clean();

    wp_send_json_success($response);
}


// This function handles all the category news, pass category name as argument
function display_ev_cate_news_vertically_shortcode($atts)
{
    $atts = shortcode_atts(
        array(
            'category' => 'รีวิว',
            'is_ev' => '1',
            'posts_per_page' => 5,
            'paged' => 1,
            'page_url' => '',
        ),
        $atts,
        'display_cate_news_vertically'
    );

    wp_enqueue_script('cate-news-script', get_stylesheet_directory_uri() . '/widget-shortcodes/ev/js/cate-news-shortcode.js', array(), null, true);

    $current_page = isset($atts['paged']) ? intval($atts['paged']) : 1;
    $news_category = $atts['category'];

    if ($news_category == 'เปรียบเทียบ') {
        $news_data = get_ev_car_comparison_news_data($current_page);
    } else {
        $news_data = get_ev_car_review_news_data($current_page);
    }

    ob_start();

    $container_id = 'cate-news-' . sanitize_title($news_category);
	if($news_category === 'เปรียบเทียบ'){
		$title = 'เปรียบเทียบรถ EV';
	}else{
		$title = 'รีวิวรถ EV';
	}
    echo '<div class="popular-ev-head"><h2 class="wa-title-text">' . $title . '</h2></div>';
    if (!empty($news_data)) { ?>
        <div class="cate-news-container" id="<?php echo $container_id; ?>">
            <ul class="good-news-list">
                <?php foreach ($news_data as $index => $news_item) :

                ?>
                    <?php if ($index == 0) : ?>
                        <li class="first-news-item news-item-first">
                            <?php if ($news_item['guid']) : ?>
                                <div class="first-news-thumbnail">
                                    <a href="<?php echo esc_url($news_item['news_href']); ?>">
                                        <img src="<?php echo esc_url($news_item['guid']); ?>" alt="<?php echo esc_attr($news_item['title']); ?>" />
                                        <div class="first-news-content">
                                            <a href="<?php echo esc_url($news_item['news_href']); ?>">
                                                <?php
                                                echo esc_html(wp_trim_words($news_item['title'], 20, '...'));
                                                ?>
                                            </a>
                                        </div>
                                    </a>
                                </div>
                            <?php endif; ?>
                        </li>
                    <?php else : ?>
                        <li class="good-news-item">
                            <div class="good-news-thumbnail">
                                <div class="image-container">
                                    <a href="<?php echo esc_url($news_item['news_href']); ?>">
                                        <img src="<?php echo esc_url($news_item['guid']); ?>" alt="<?php echo esc_attr($news_item['title']); ?>" />
                                    </a>
                                </div>
                            </div>
                            <div class="good-news-content col-md-12">
                                <a href="<?php echo esc_url($news_item['news_href']);  ?>">
                                    <?php echo esc_html(wp_trim_words($news_item['title'], 13, '...')); ?></a>
                                <div class="good-news-meta">
                                    <div class="name-date ">
                                        <span class="author-name"><?php echo esc_html($news_item['author']);  ?></span>
                                        <span class="good-news-date"><?php echo esc_html($news_item['publish_time']); ?></span>
                                    </div>
                                </div>
                            </div>
                        </li>
                <?php endif;
                endforeach; ?>
            </ul>

        
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
        </style>
    <?php } else { ?>
        <p>No news available.</p>
<?php }
    wp_reset_postdata();

    return ob_get_clean();
}

// change the shortcode name to 'display_cate_news_vertically'
add_shortcode('display_ev_cate_news_vertically', 'display_ev_cate_news_vertically_shortcode');
