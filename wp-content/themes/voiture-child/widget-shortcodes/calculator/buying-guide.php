<?php
function buying_guides_shortcode($atts)
{
    $news_items = get_category_news_data('Buying Guide');
    if (empty($news_items)) {
        return '';
    }

    wp_reset_postdata();

    ob_start();
?>
    <h2 style="margin-bottom: 16px;" class="wa-title-text">Buying Guides</h2>
    <div id="buying-guide-news-container" style="list-style: none; padding: 0;">

        <?php foreach ($news_items as $item): ?>
            <div class="buying-guide-news-item" style="display: flex; padding: 10px 0; border-bottom: 1px solid #e0e0e0; gap: 6px; align-items: flex-start;">
                <div class="news-thumbnail">
                    <a href="<?php echo esc_url($item['link']); ?>">
                        <img style="width: 107px; height: 97%; border-radius: 5px; object-fit: cover;"
                            src="<?php echo esc_url($item['thumbnail_url']); ?>"
                            alt="<?php echo esc_attr($item['title']); ?>" />
                    </a>
                </div>
                <div class="news-info" style="flex: 1;">
                    <h3 style="font-size: 16px; font-weight: bold; color: #262626; margin: 0 0 4px; line-height: 22px; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-box-orient: vertical; -webkit-line-clamp: 2;">
                        <a class="news-title-buing-guide" href="<?php echo esc_url($item['link']); ?>" style="color: inherit; text-decoration: none;">
                            <?php echo esc_html($item['title']); ?>
                        </a>
                    </h3>
                    <div class="news-meta">
                        <?php echo esc_html($item['author']); ?> • <?php echo esc_html($item['post_date']); ?>
                    </div>
                </div>
            </div>
        <?php endforeach; ?>
    </div>

    <div id="view-more-container" style="text-align: center; margin-top: 20px;">
        <button class="view-more" id="view-more-buying-guide-news" data-page="2" onclick="window.location.href='<?php echo home_url('/news/buying-guides'); ?>';">
            View More
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" style="transform: rotate(90deg);">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
        </button>
        <!-- <div id="loading" style="display: none; margin-top: 10px;">Loading...</div> -->
    </div>

    <script>
        // AJAX View More Logic
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
                        action: 'load_more_category_news',
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

add_shortcode('buying_guides_shortcode', 'buying_guides_shortcode');
