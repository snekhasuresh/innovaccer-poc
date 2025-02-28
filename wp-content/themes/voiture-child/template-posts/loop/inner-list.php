<?php
global $post;
$thumbsize = !isset($thumbsize) ? voiture_get_config('blog_item_thumbsize', 'full') : $thumbsize;
$thumb = voiture_display_post_thumb($thumbsize);
$hasthumb = !empty($thumb) ? '' : ' nothumb';
$post_thumbnail_id = get_post_thumbnail_id($post->ID);
$thumbnail_post = get_post($post_thumbnail_id);
$guid = $thumbnail_post->guid;
$author_id = get_post_field('post_author', $post->ID);
$author_image_url = get_the_author_meta('user_url', $author_id);

if (!$author_image_url) {
    $author_image_url = get_avatar_url($author_id, ['size' => 32]);
}
$default_link = get_permalink($post->ID);
$custom_link = get_custom_post_link($post->ID, $default_link);

$author_page_link = get_author_posts_url($author_id);
$custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

?>
<article style="margin-bottom: 10px;" <?php post_class('post post-layout post-list-item' . $hasthumb); ?>>
    <div class="flex-middle">
        <?php
        if (!empty($thumb)) {
        ?>
            <div class="top-image">
                <a href="<?php echo esc_url($custom_link); ?>">
                    <?php voiture_post_categories_first($post);
                    echo '<img src="' . esc_url($guid) . '" alt="Image" />'; ?>
                </a>
            </div>
        <?php
        }
        ?>
        <div class="col-content">
            <?php if (empty($thumb)) { ?>
                <?php voiture_post_categories_first($post); ?>
            <?php } ?>
            <div class="top-detail-info clearfix">
                <a href="<?php echo esc_url($custom_author_link); ?>">
                    <!--                     <i class="flaticon-user"></i> -->
                    <span class="authour-detail">
                        <img class="news-profile-images" src="<?php echo esc_url($author_image_url); ?>" />
                        <span class="authour-name"><?php echo get_the_author(); ?> </span>
                    </span>
                </a>

                <span class="date article-date">
                    <i class="flaticon-calendar-1"></i>
                    <?php the_time(get_option('date_format', 'd M, Y')); ?>
                </span>
            </div>
            <?php if (get_the_title()) { ?>
                <h4 class="entry-title">
                    <?php if (is_sticky() && is_home() && ! is_paged()) : ?>
                        <div class="stick-icon"><i class="ti-pin2"></i></div>
                    <?php endif; ?>

                    <a href="<?php echo esc_url($custom_link); ?>"><?php the_title(); ?></a>
                </h4>
            <?php } ?>
            <div class="description news-description-max visible-lg"><?php echo voiture_substring(get_the_excerpt(), 24, '...'); ?></div>
            <div class="description hidden-lg"><?php echo voiture_substring(get_the_excerpt(), 9, '...'); ?></div>

            <div class="more-bottom hidden-xs">
                <a href="<?php echo esc_url($custom_link); ?>" class="btn-readmore flex-middle"><?php echo esc_html__('Read More', 'voiture') ?> <span class="plus read-more-button" style="background-color: #2e2e2e;">
                        <svg width="11" height="11" viewBox="0 0 11 11" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                            <path d="M5 0H6V11H5V0Z" />
                            <path d="M4.37113e-08 6L0 5L11 5V6L4.37113e-08 6Z" />
                        </svg>
                    </span></a>
            </div>
        </div>
    </div>
    <style>
  .entry-title a {
            font-size: 26.91px !important;
            text-align: left;
            transition: all .2s;
            display: -webkit-box;
            overflow: hidden;
            text-overflow: ellipsis;
            -webkit-box-orient: vertical;
            -webkit-line-clamp: 2;
            color: #262626 !important;
            line-height: 1.3em;
            font-family: "Roboto Condensed";
            letter-spacing: -0.02em;
            font-weight: 700;
            word-break: normal;
            overflow-wrap: anywhere;
	}
	
	.news-description-max{
    		display: -webkit-box !important;
    		-webkit-line-clamp: 2;
	}

        .entry-title a:hover {
            color: #ffb400 !important;
        }

        .entry-title:hover {
            color: #ffb400 !important;
        }
		.entry-title:hover::after {
            color: #262626 !important;
        }
		.col-content a:active {
		  color: #262626;
		}
		.col-content a:focus {
		  color: #262626;
		}
        .description {
            margin-top: 4px;
            font-size: 14px;
            font-family: "Roboto";
            color: #8c8c8c;
            line-height: 22px;
/*             display: -webkit-box !important; */
            overflow: hidden;
            text-overflow: ellipsis;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            word-break: break-word;
        }

        .top-image img {
            height: 200px;
            width: 300px;
        }
		.post-list-item .top-image {
        	width: 300px;
    	}
		.col-content {
			height:200px !important;
		}
		
		.col-content .description {
			margin-top:5px;
		}
		.post-layout .more-bottom {
        	margin-top: 8px;
    	}
		@media screen and (max-width: 768px) {
			.top-image img {
				height: 238px !important;
				width: 100% !important;
			}
			.news-description-max{
    				display:none;
    				-webkit-line-clamp: 2;
			}
		}
    </style>
</article>
