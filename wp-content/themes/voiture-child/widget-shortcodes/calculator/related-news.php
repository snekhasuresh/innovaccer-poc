<?php
function related_news_shortcode($atts)
{
    $tag = $atts['tag'];

    $news_data = get_related_tag_news_data($tag, 1);
    $posts_per_page = 5;
    $news_count = count($news_data);

    ob_start();
    if (!empty($news_data)) {

?>
        <h2 style="margin-bottom: 16px;" class="wa-title-text">Related News</h2>
        <div id="related-news-container" style="list-style: none; padding: 0;">
            <?php foreach ($news_data as $data) { ?>
                <div class="buying-guide-news-item" style="display: flex; padding: 6px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                    <div class="news-thumbnail">
                        <a href="<?php echo esc_url($data['link']); ?>">
                            <img src="<?php echo esc_url($data['thumbnail_url']); ?>" style="width: 100px; height: 56px; border-radius: 5px; object-fit: cover;" />
                        </a>
                    </div>
                    <div class="news-info" style="flex: 1;">
                        <h3 style="font-size: 16px; font-weight: bold; color: #333; margin: 0 0 4px; line-height: 1.1; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2;">
                            <a href="<?php echo esc_url($data['link']); ?>" style="color: inherit; text-decoration: none;">
                                <?php echo esc_html($data['title']); ?>
                            </a>
                        </h3>
                        <div class="news-meta">
                            <?php echo esc_html($data['author']); ?> • <?php echo esc_html($data['post_date']); ?>
                        </div>
                    </div>
                </div>
            <?php } ?>
        </div>

        <?php if ($news_count > $posts_per_page) : ?>
            <div id="view-more-container" style="text-align: center; margin-top: 20px;">
                <button class="view-more-btn" id="view-more-news" data-page="2">
                    View More
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" style="transform: rotate(90deg);">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                    </svg>
                </button>
                <div id="loading" style="display: none; margin-top: 10px;">Loading...</div>
            </div>
        <?php endif; ?>
        <style>
            .view-more-btn {
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

            .view-more-btn svg {
                width: 18px;
                height: 20px;
            }
        </style>
        <script>
            jQuery(document).ready(function($) {
                $('#view-more-news').off('click').on('click', function() {
                    var button = $(this);
                    var page = button.data('page');
                    var loading = $('#loading');
                    loading.show();

                    $.ajax({
                        url: '<?php echo admin_url('admin-ajax.php'); ?>',
                        type: 'GET',
                        data: {
                            action: 'load_more_related_news',
                            page: page,
                            tag: '<?php echo $tag; ?>'
                        },
                        cache: false, // Prevent caching of the request
                        success: function(response) {
                            var data = JSON.parse(response); // Parse the JSON response
                            if (data.content.length > 0) {
                                // Append the new posts to the container
                                $('#related-news-container').append(data.content.join(''));
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
                        error: function(xhr, status, error) {
                            console.log("AJAX Error:", error);
                            console.log("AJAX Response:", xhr.responseText);
                            alert('Error loading more posts.');
                            loading.hide();
                        }
                    });
                });
            });
        </script>

    <?php
    }
    wp_reset_postdata();
    return ob_get_clean();
}
add_shortcode('related_news_shortcode', 'related_news_shortcode');

function load_more_related_news()
{
    $tag = isset($_GET['tag']) ? intval($_GET['tag']) : 'Buying Guide';
    if (!$tag) {
        echo '<p>Error: Term ID not received.</p>';
        wp_die();
    }

    $paged = isset($_GET['page']) ? intval($_GET['page']) : 1;

    $news_data = get_related_tag_news_data($tag, $paged);

    wp_reset_postdata();

    ob_start();
    if (!empty($news_data)) {
    ?>
        <?php foreach ($news_data as $data) { ?>
            <div class="buying-guide-news-item" style="display: flex; padding: 6px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                <div class="news-thumbnail">
                    <a href="<?php echo esc_url($data['link']); ?>">
                        <img style="width: 100px; height: 56px; border-radius: 5px; object-fit: cover;" src="<?php echo esc_url($data['thumbnail_url']); ?>" />
                    </a>
                </div>
                <div class="news-info" style="flex: 1;">
                    <h3 style="font-size: 16px; font-weight: bold; color: #333; margin: 0 0 4px; line-height: 1.1; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2;">
                        <a href="<?php echo esc_url($data['link']); ?>" style="color: inherit; text-decoration: none;">
                            <?php echo esc_html($data['title']); ?>
                        </a>
                    </h3>
                    <div class="news-meta">
                        <?php echo esc_html($data['author']); ?> • <?php echo esc_html($data['post_date']); ?>
                    </div>
                </div>
            </div>
<?php
        }
        $response[] = ob_get_clean();
        $show_more = count($news_data) >= 5;
        wp_reset_postdata();

        echo json_encode(array('content' => $response, 'show_more' => $show_more));
    } else {
        echo json_encode(array('content' => '', 'show_more' => false));
    }
    wp_die();
}
add_action('wp_ajax_load_more_related_news', 'load_more_related_news');
add_action('wp_ajax_nopriv_load_more_related_news', 'load_more_related_news');
