<?php
function motor_buying_guides_shortcode($atts)
{
    $buying_guide_term = get_term_by('name', 'ท็อปแรงกิ้ง', 'motorcycle-news-category');

    if (!$buying_guide_term || is_wp_error($buying_guide_term)) {
        return '<p>Buying guide category not found.</p>';
    }
    $buying_guide_term_id = $buying_guide_term->term_id;

    $paged = isset($atts['paged']) ? intval($atts['paged']) : 1;

    $args = array(
        'post_type' => 'motorcycle-news',
        'posts_per_page' => 5,
        'tax_query' => array(
            array(
                'taxonomy' => 'motorcycle-news-category',
                'field' => 'term_id',
                'terms' => $buying_guide_term_id, // Use term IDs directly
                'include_children' => true, // Include subcategories
            ),
        ),
        'meta_query' => array(
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            ),
        ),
    );

    $news_query = new WP_Query($args);

    if ($news_query->have_posts()) {
        $news_post = [];
        while ($news_query->have_posts()) : $news_query->the_post();
            $thumbnail_id = get_post_meta(get_the_ID(), '_thumbnail_id', true);
            $image_post = get_post($thumbnail_id);
            $image_guid = $image_post ? $image_post->guid : '';

            $news_post[] = [
                'id' => get_the_ID(),
                'title' => get_the_title(),
                'link' => get_permalink(),
                'thumbnail_url' => $image_guid,
                'author' => get_the_author_meta('display_name'),
                'date' => get_the_date('F j, Y'),
            ];
        endwhile;
    }

    ob_start();
?>
    <h2 style="margin-bottom: 16px;" class="wa-title-text">คู่มือซื้อรถ</h2>
    <div id="buying-guide-news-container" style="list-style: none; padding: 0;">

        <?php if (!empty($news_post)):
            foreach ($news_post as $post_data) : ?>

                <div class="buying-guide-news-item" style="display: flex; padding: 6px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                    <div class="news-thumbnail">
                        <a href="<?php echo esc_url($post_data['link']); ?>">
                            <img style="width: 107px;height: 97%;border-radius: 5px;object-fit: cover;" src="<?php echo esc_url($post_data['thumbnail_url']); ?>" alt="<?php echo esc_attr($post_data['title']); ?>" style="width: 100px; height: 56px; border-radius: 5px;" />
                        </a>
                    </div>
                    <div class="news-info" style="flex: 1;">
                        <h3 style="font-size: 16px; font-weight: bold; color: #333; margin: 0 0 4px; line-height: 1.1; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2;">
                            <a href="<?php echo esc_url($post_data['link']); ?>" style="color: inherit; text-decoration: none;">
                                <?php echo esc_html($post_data['title']); ?>
                            </a>
                        </h3>
                        <!-- Author and Date -->
                        <div class="news-meta">
                            <?php echo esc_html($post_data['author']); ?> • <?php echo esc_html($post_data['date']); ?>
                        </div>
                    </div>
                </div>

            <?php endforeach; ?>
        <?php endif; ?>

    </div>

    <?php if ($news_query->max_num_pages > $paged): ?>
        <div id="view-more-container" style="text-align: center; margin-top: 20px;">
            <button class="view-more" id="view-more-buying-guide-news" data-page="2">
                ดูเพิ่มเติม
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" style="transform: rotate(90deg);">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
            </button>
            <div id="loading" style="display: none; margin-top: 10px;">Loading...</div>
        </div>
    <?php endif; ?>

    <script>
        jQuery(document).ready(function($) {
            $('#view-more-buying-guide-news').off('click').on('click', function() {
                var button = $(this);
                var page = button.data('page');
                var loading = $('#loading');
                loading.show();

                $.ajax({
                    url: '<?php echo admin_url("admin-ajax.php"); ?>',
                    type: 'GET',
                    data: {
                        action: 'load_more_motor_category_news',
                        page: page,
                        term_id: <?php echo $buying_guide_term_id; ?>,
                    },
                    success: function(response) {
                        var data = JSON.parse(response); // Parse the JSON response
                        if (data.content.length > 0) {
                            // Append the new posts to the container
                            $('#buying-guide-news-container').append(data.content.join(''));
                            button.data('page', page + 1);

                            // Show or hide the "View More" button based on the show_more flag
                            if (data.show_more) {
                                button.show();
                            } else {
                                button.hide();
                            }
                        } else {
                            button.hide();
                        }
                        loading.hide();
                    },
                    error: function() {
                        loading.hide();
                        alert('Failed to load more news');
                    }
                });
            });
        })(jQuery);
    </script>
    <style>
        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }
    </style>

    <?php
    wp_reset_postdata();
    return ob_get_clean();
}

add_shortcode('motor_buying_guides_shortcode', 'motor_buying_guides_shortcode');

function load_more_motor_category_news_ajax()
{
    $paged = isset($_GET['page']) ? intval($_GET['page']) : 1;
    $term_id = isset($_GET['term_id']) ? intval($_GET['term_id']) : 0;

    $args = array(
        'post_type' => 'motorcycle-news',
        'posts_per_page' => 5,
        'paged' => $paged,
        'tax_query' => array(
            array(
                'taxonomy' => 'motorcycle-news-category',
                'field' => 'term_id',
                'terms' => $term_id, // Use term IDs directly
                'include_children' => true, // Include subcategories
            ),
        ),
        'meta_query' => array(
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            ),
        ),
    );

    $news_query = new WP_Query($args);
    $response = array();

    if ($news_query->have_posts()) {
        while ($news_query->have_posts()) {
            $news_query->the_post();
            $news_title = get_the_title();
            $news_permalink = get_permalink();
            $news_author = get_the_author();
            $news_date = get_the_date('d.m.Y');
            ob_start();
    ?>

            <div class="buying-guide-news-item" style="display: flex; padding: 6px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                <div class="news-thumbnail">
                    <?php if (has_post_thumbnail()):
                        $post_thumbnail_id = get_post_thumbnail_id(get_the_ID()); // Get the post thumbnail ID
                        $thumbnail_post = get_post($post_thumbnail_id); // Retrieve the post object for the thumbnail
                        $guid = $thumbnail_post->guid;
                    ?>
                        <a href="<?php echo esc_url($news_permalink); ?>">
                            <img style="width: 107px;height: 97%;border-radius: 5px;object-fit: cover;" src="<?php echo esc_url($guid); ?>" alt="<?php echo esc_attr($thumbnail_post->post_title); ?>" style="width: 100px; height: 56px; border-radius: 5px;" />
                        </a>
                    <?php endif; ?>
                </div>
                <div class="news-info" style="flex: 1;">
                    <h3 style="font-size: 16px; font-weight: bold; color: #333; margin: 0 0 4px; line-height: 1.1; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2;">
                        <a href="<?php echo esc_url($news_permalink); ?>" style="color: inherit; text-decoration: none;">
                            <?php echo esc_html($news_title); ?>
                        </a>
                    </h3>
                    <div class="news-meta" style="font-size: 10px; color: #777;">
                        <?php echo esc_html($news_author); ?> • <?php echo esc_html($news_date); ?>
                    </div>
                </div>
            </div>
<?php
            $response[] = ob_get_clean();
        }
        $show_more = ($news_query->max_num_pages > $paged) ? true : false;
        wp_reset_postdata();

        echo json_encode(array('content' => $response, 'show_more' => $show_more));
    } else {
        echo json_encode(array('content' => '', 'show_more' => false));
    }
    wp_die();
}

add_action('wp_ajax_load_more_motor_category_news', 'load_more_motor_category_news_ajax');
add_action('wp_ajax_nopriv_load_more_motor_category_news', 'load_more_motor_category_news_ajax');
