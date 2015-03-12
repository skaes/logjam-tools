#ifndef __STR_BUILDER_H_INCLUDED__
#define __STR_BUILDER_H_INCLUDED__

typedef struct _str_builder str_builder;

str_builder* sb_new(size_t size);

char *sb_string(str_builder *sb);

void sb_append(str_builder *sb, const char* str, size_t length);

void sb_destroy(str_builder **sb);

#endif
