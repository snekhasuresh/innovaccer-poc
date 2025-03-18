<?php
function fuel_price_news_shortcode($atts)
{
    $args = array(
        'post_type' => 'news',
        'posts_per_page' => 3, // You can change this number to display more or fewer posts
        'orderby' => 'post_modified',
        'order' => 'DESC'
    );
    $news_posts = new WP_Query($args);

    // Start output buffering
    ob_start(); ?>

    <div id="latest-fuel-price-news-section" class="latest-news-section inner">
        <h2 class="wa-title-text"><?php esc_html_e('Latest Fuel Price News', 'voiture'); ?></h2>
        <div class="latest-fuel-news-cards">
            <?php while ($news_posts->have_posts()) {
                $news_posts->the_post();
                $thumbnail_id = get_post_thumbnail_id();

                // Get the image URL and GUID
                $image_url = wp_get_attachment_image_src($thumbnail_id, 'thumbnail')[0]; // Smaller thumbnail image
                $image_guid = get_post($thumbnail_id)->guid; // Get the GUID of the image
                $news_href = get_permalink(); ?>

                <div class="latest-fuel-news-card">
                    <img src="<?php echo esc_url($image_guid); ?>" alt="News Image" data-guid="<?php echo esc_url($image_guid); ?>">
                    <div class="latest-fuel-news-content">
                        <a href="<?php echo esc_url($news_href); ?>" class="latest-fuel-news-title"><?php echo esc_html(get_the_title()); ?></a>
                        <div class="latest-fuel-news-meta">
                            <span class="latest-author"><?php esc_html_e('By', 'voiture'); ?> <?php the_author(); ?></span>
                            <span class="latest-date"><?php echo esc_html(get_the_date('d.m.Y')); ?></span>
                        </div>
                    </div>
                </div>

            <?php }
            wp_reset_postdata(); ?>
        </div>

        <style>
            #latest-fuel-price-news-section {
                margin: 20px 0;
            }

            .latest-fuel-news-cards {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }

            .latest-fuel-news-card {
                display: flex;
                align-items: center;
                background: #fff;
                border-bottom: 1px solid #e0e0e0;
                padding-bottom: 10px;
            }

            .latest-fuel-news-card img {
                width: 110px;
                height: 70px;
                object-fit: cover;
                border-radius: 5px;
                margin-right: 10px;
            }

            .latest-fuel-news-content {
                display: flex;
                flex-direction: column;
            }

            .latest-fuel-news-title {
                font-size: 14px;
                font-weight: bold;
                color: #333;
                text-decoration: none;
                line-height: 1.2;
                display: block;
                margin-bottom: 5px;
                max-height: 2.8em;
                /* Restricts to 2 lines */
                overflow: hidden;
                text-overflow: ellipsis;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                /* Limits to 2 lines */
                -webkit-box-orient: vertical;
                font-family: "Roboto";
            }

            .latest-fuel-news-title:hover {
                color: #071A40;
            }

            .latest-fuel-news-meta {
                font-size: 12px;
                color: #999;
                margin-top: 5px;
            }

            .latest-fuel-news-meta .latest-author {
                margin-right: 10px;
            }

            .latest-fuel-news-meta .latest-date {
                color: #666;
            }

            @media (max-width: 768px) {
                .latest-fuel-news-cards {
                    gap: 8px;
                }

                .latest-fuel-news-card img {
                    width: 50px;
                    height: 50px;
                }

                .latest-fuel-news-title {
                    font-size: 12px;
                }

                .latest-fuel-news-meta {
                    font-size: 10px;
                }
            }
        </style>
    </div>

<?php
    return ob_get_clean();
}
add_shortcode('fuel_price_news', 'fuel_price_news_shortcode');
